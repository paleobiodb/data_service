 
# The Paleobiology Database
# 
#   Taxonomy.pm
# 

package Taxonomy;

use TaxonTrees;

use Carp qw(carp croak);
use Try::Tiny;

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

our ($ANCESTRY_SCRATCH) = 'ancestry_aux';

our ($SQL_STRING);

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
		 attrs_table => $TaxonTrees::ATTRS_TABLE{$table_name},
		 search_table => $TaxonTrees::SEARCH_TABLE{$table_name},
		 opinion_table => $TaxonTrees::OPINION_TABLE{$table_name},
		 opinion_cache => $TaxonTrees::OPINION_CACHE{$table_name} };
    
    bless $self, $class;
    
    return $self;
}


# The following expressions list the various sets of fields that will be
# returned as part of a Taxon object:

# The "basic" fields are always returned.

our ($AUTH_BASIC_FIELDS) = "a.taxon_name, a.taxon_no, a.taxon_rank, a.common_name, a.extant, a.orig_no, o.status, a.reference_no";

our ($OPINION_BASIC_FIELDS) = "o.opinion_no, o.reference_no, o.status, o.phylogenetic_status, o.spelling_reason, o.child_no, o.child_spelling_no, o.parent_no, o.parent_spelling_no";

# The "attribution" fields are returned additionally if we are asked for 'attr'.

our ($ATTR_FIELDS) = ", if(a.refauth, r.author1last, a.author1last) as a_al1, if(a.refauth, r.author1init, a.author1init) as a_ai1, if(a.refauth, r.author2last, a.author2last) as a_al2, if(a.refauth, r.author2init, a.author2init) as a_ai2, if(a.refauth, r.otherauthors, a.otherauthors) as a_ao, if(a.refauth, r.pubyr, a.pubyr) as a_pubyr";

our ($OPINION_ATTR_FIELDS) = ", if(o.refauth, r.author1last, o.author1last) as a_al1, if(o.refauth, r.author1init, o.author1init) as a_ai1, if(o.refauth, r.author2last, o.author2last) as a_al2, if(o.refauth, r.author2init, o.author2init) as a_ai2, if(o.refauth, r.otherauthors, o.otherauthors) as a_ao, if(o.refauth, r.pubyr, o.pubyr) as a_pubyr";

# The "old attribution" fields are the same but under the names expected by the
# old code.  They are returned if we are asked for 'oldattr'.

our ($OLDATTR_FIELDS) = ", if(a.refauth, r.author1last, a.author1last) as author1last, if(a.refauth, r.author2last, a.author2last) as author2last, if(a.refauth, r.otherauthors, a.otherauthors) as otherauthors, if(a.refauth, r.pubyr, a.pubyr) as pubyr, a.ref_is_authority";

our ($OPINION_OLDATTR_FIELDS) = ", if(o.refauth, r.author1last, o.author1last) as author1last, if(o.refauth, r.author2last, o.author2last) as author2last, if(o.refauth, r.otherauthors, o.otherauthors) as otherauthors, if(o.refauth, r.pubyr, o.pubyr) as pubyr, o.ref_has_opinion";

# The "reference" fields are returned additionally if we are asked for 'ref'.

our ($REF_FIELDS) = ", r.author1init as r_ai1, r.author1last as r_al1, r.author2init as r_ai2, r.author2last as r_al2, r.otherauthors as r_oa, r.pubyr as r_pubyr, r.reftitle as r_reftitle, r.pubtitle as r_pubtitle, r.editors as r_editors, r.pubvol as r_pubvol, r.pubno as r_pubno, r.firstpage as r_fp, r.lastpage as r_lp";

# The "link" fields are returned additionally if we are asked for 'link'.

our ($LINK_FIELDS) = ", t.spelling_no, t.synonym_no, t.parent_no";

# The "orig" fields are returned additionally if we are asked for 'orig'.

our ($ORIG_FIELDS) = ", ora.taxon_name as orig_name, ora.taxon_rank as orig_rank";

# The "lft" fields are "lft" and "rgt".  They delineate each taxon's position
# in the hierarchy.

our ($LFT_FIELDS) = ", t.lft, t.rgt";

# The "parent" fields are returned if we are asked for 'parent'.

our ($PARENT_FIELDS) = ", pa.taxon_name as parent_name, pa.taxon_rank as parent_rank";

# The "child" fields are returned if we are asked for 'child'.

our ($CHILD_FIELDS) = ", ca.taxon_name as child_name, ca.taxon_rank as child_rank";

# The "kingdom" field is returned if we are asked for 'kingdom'.

our ($KINGDOM_FIELDS) = ", t.kingdom";

# The "type" field is returned if we are asked for 'tt'.

our ($TT_FIELDS) = ", tta.taxon_no as type_taxon_no, tta.taxon_name as type_taxon_name, tta.taxon_rank as type_taxon_rank, a.type_locality";

# The "person" fields are returned if we are asked for 'person'.

our ($PERSON_FIELDS) = ", a.authorizer_no, a.enterer_no, a.modifier_no, pp1.name as authorizer_name, pp2.name as enterer_name, pp3.name as modifier_name";

# The "specimen" fields are returned if we are asked for 'specimen'.

our ($SPECIMEN_FIELDS) = ", a.type_specimen, a.type_body_part, a.form_taxon, a.part_details, a.preservation";

# The "pages" fields are returned if we are asked for 'pages'.

our ($PAGES_FIELDS) = ", a.pages, a.figures";

# The "created" fields are returned if we are asked for 'created'.

our ($CREATED_FIELDS) = ", a.created, DATE_FORMAT(a.modified,'%Y-%m-%e %H:%i:%s') modified";

our ($OPINION_CREATED_FIELDS) = ", o.created, DATE_FORMAT(o.modified,'%Y-%m-%e %H:%i:%s') modified";

# The "modshort" fields are returned if we are asked for 'modshort'.

our ($MODSHORT_FIELDS) = ", DATE_FORMAT(a.modified,'%m/%e/%Y') modified_short";

our ($OPINION_MODSHORT_FIELDS) = ", DATE_FORMAT(o.modified,'%m/%e/%Y') modified_short";

# The "comment" fields are returned if we are asked for 'comments'.

our ($COMMENT_FIELDS) = ", a.comments";

# The following hash is used by &getTaxonIdTable

our(%TAXON_FIELD) = ('lft' => 1, 'rgt' => 1, 'depth' => 1, 'opinion_no' => 1,
		     'spelling_no' => 1, 'trad_no' => 1,
		     'synonym_no' => 1, 'parent_no' => 1);


=head2 Object Methods

The following methods (except for C<getTaxonIdTable>) can return either a list
of Taxon objects or a list of taxon_no values.  Each of these takes an options
hash as its last argument.  The options can include the following, plus any
options listed under the individual method headings:

=over 4

=item fields

This option allows you to specify additional fields to be included in the
resulting Taxon objects.  Its value should be a list of one or more of
the following strings:

=over 4

=item attr

Includes the fields 'a_al1', 'a_al2', 'a_ao' and 'a_pubyr', which provide the
last names of the authors of the taxon and the year in which the taxon was
first published.

=item oldattr

Includes the fields 'author1last', 'author2last', 'otherauthors' and 'pubyr',
which provide the last names of the authors of the taxon and the year in which
the taxon was first published.  These fields contain the same data as with
'attr', but under the names expected by the old code.

=item ref

Includes the fields 'r_ai1', 'r_al1', 'r_ai2', 'r_al2', 'r_oa', 'r_pubyr',
'r_reftitle', 'r_pubtitle', 'r_editors', 'r_pubvol', 'r_pubno', 'r_fp', and
'r_lp'.  These provide the first and last names of the authors of the reference
in which the taxon was first published, along with the publication information.

=item link

Includes the fields 'spelling_no', 'synonym_no', and 'parent_no', which
indicate how the taxon is related to the rest of this taxonomic hierarchy.

=item lft

Includes the fields 'lft' and 'rgt', which delineates each taxon's position in
the hierarchy.

=item parent

Includes the fields 'parent_name' and 'parent_rank', which describe each
taxon's parent in the hierarchy.  (For opinions, these fields describe the
taxon being assigned to).

=item child

Includes the fields 'child_name' and 'child_rank', which are only valid for
opinions.  These fields describe the taxon being assigned.

=item kingdom

Includes the field 'kingdom', indicating the kingdom of life into which each
taxon is placed.

=item tt

Includes the fields 'type_taxon_no', 'type_taxon_name' and 'type_taxon_rank',
which describe each taxon's type taxon (if any).

=item person

Includes the fields 'authorizer_no', 'authorizer_name', 'enterer_no',
'enterer_name', 'modifier_no', 'modifier_name', which describe the people who
have created/touched each record.

=item specimen

Includes the fields 'type_specimen', 'type_body_part', 'part_details' and
'preservation', which describe the type specimen on which each taxon is based.

=item pages

Includes the fields 'pages' and 'figures', which indicate where in the
referring publication each taxon is described.

=item created

Includes the fields 'created' and 'modified', which indicate when the record
for each taxon was created and when it was last modified.

=item modshort

Includes the field 'modshort', indicating the day (exclusive of time) on which
each taxon's record was last modified.

=item comments

Includes the field 'comments'.

=back

The keys 'attr', 'oldattr', 'ref', 'parent', 'child', 'person', 'pages',
'created', 'modshort' and 'comments' can be used with this option in the
context of a call to getOpinions().  The others are invalid.

=item id

If this option is specified, a list of distinct taxon_no values will be
returned instead of a list of Taxon objects.  In that case, the 'fields'
option is ignored.

=item hash

If this option is specified, instead of a list of Taxon objects the return
value will be a hashref whose keys are taxon_nos and whose values are Taxon
objects.  This option is ignored if 'id' is also specified.  The method
C<getRelatedTaxa> adds a special value for this option, refer to the
documentation below for details.

=back


=head3 getTaxon ( taxon_no, options )

Returns a Taxon object corresponding to the given taxon_no.  This is a
convenience routine that calls C<getRelatedTaxon> with the relationship 'self'.

=cut

sub getTaxon {

    my ($self, $taxon_no, $options) = @_;
    
    return $self->getRelatedTaxon($taxon_no, 'self', $options);
}


=head3 getTaxaByName ( name, options )

Returns a list of objects of class C<Taxon> representing all concepts in this
taxonomy having one or more spellings which match the given name.  If no
matching taxa are found, returns the empty list.  If called in scalar context,
returns the first taxon found.  The name may include the SQL wildcard
characters % and _.  Any periods appearing in the name are also treated as
wildcards of the % type.

    @taxa = $taxonomy->getTaxaByName('Ficus', { kingdom => 'metazoa' });
    @taxa = $taxonomy->getTaxaByName('F. bernardi');

If the option 'id' is specified, returns a list of taxon identifiers
instead.  Options include those specified above, plus:

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

=item exact

This item is only relevant for genus and below.  If specified, the genus
portion of the name is not matched against subgenera.

=item common

If specified, the name is matched against the common name of teach taxon as
well as the scientific name.

=item order

If specified, the list of names is ordered by the specified criteria.  If
'.desc' is appended to the value, they are ranked in descending order.
Otherwise, they are ranked in ascending order.  Possible values include:

=over 4

=item name

Taxa are ordered alphabetically by name.

=item size

Taxa are ordered by size (number of descendants).

=item lft

Taxa are ordered by tree sequence number (which guarantees parents before children).

=back

=item select

This option determines how taxonomic concepts are treated.  The possible
values are as follows:

=over 4

=item spelling

This is the default, and returns one Taxon object for each concept which has a
matching spelling, representing the curently accepted spelling of that concept
(even if the currently accepted spelling doesn't match).

=item orig

Returns one Taxon object for each concept which has a matching spelling,
representing the original spelling of that concept (even if the original
spelling doesn't match).

=item all

Returns one Taxon object for each matching spelling.

=back

=back

=cut

sub getTaxaByName {

    my ($self, $taxon_name, $options) = @_;
    
    # Check the arguments.  Carp a warning and return undefined if name is not
    # defined or is empty.  Treat a wildcard '%' as empty; if you want to get
    # all of the taxa in the database, you should use getTaxonIdTable() with
    # the 'all_taxa' parameter instead.
    
    unless ( defined $taxon_name and $taxon_name ne '' and $taxon_name ne '%' )
    {
	carp "taxon name is undefined or empty";
	return;
    }
    
    # Set option defaults.
    
    $options ||= {};
    
    my $status = defined $options->{status} && $options->{status} ne '' ?
	lc $options->{status} : 'valid';
    my $select = defined $options->{select} ? lc $options->{select} : 'spelling';
    
    unless ( $select eq 'all' or $select eq 'spelling' 
	     or $select eq 'orig' or $select eq 'trad' )
    {
	croak "invalid value '$options->{select}' for option 'select'";
    }
    
    $select = 'trad' if $options->{trad} and $select eq 'spelling';
    
    # Construct the appropriate modifiers based on the options.  If we can't
    # parse the name, return undefined.
    
    my (@filter_list, @param_list);
    my ($genus, $subgenus, $species, $subspecies, $search_name, $search_rank);
    
    # If we were given an array or hash of names, simply search for all of
    # them with no special handling.
    
    if ( ref $taxon_name eq 'ARRAY' or ref $taxon_name eq 'HASH' )
    {
	my @name_list = (ref $taxon_name eq 'ARRAY' ? @$taxon_name : keys %$taxon_name);
	
	# take out everything except alphabetic characters and spaces
	tr{a-zA-Z}{}cd foreach @name_list;
	# join the names together into a single expression
	push @filter_list, "a.taxon_name in ('" . join("','", @name_list) . "')";
    }
    
    elsif ( ref $taxon_name eq 'HASH' )
    {
	# take out everything except alphabetic characters and spaces
	tr{a-zA-Z}{}cd foreach keys %$taxon_name;
	# join the names together into a single expression
	push @filter_list, "a.taxon_name in ('" . join("','", keys %$taxon_name) . "')";
    }
    
    # If the option 'exact' was specified, we do the same even if only a
    # single name was given.
    
    elsif ( $options->{exact} )
    {
	push @filter_list, "a.taxon_name = '$taxon_name'";
    }
    
    # Otherwise, try to parse the taxon name according to the usual rules.
    
    elsif ($taxon_name =~ /^([A-Za-z.%]+)(?:\s+\(([A-Za-z.%]+)\))?(?:\s+([A-Za-z.%]+))?(?:\s+([A-Za-z.%]+))?/)
    {
        $genus = $1;
        $subgenus = $2 if defined $2;
        $species = $3 if defined $3;
        $subspecies = $4 if defined $4;
	
	$genus =~ s/\./%/g;
	$subgenus =~ s/\./%/g if defined $subgenus;
	$species =~ s/\./%/g if defined $species;
	$subspecies =~ s/\./%/g if defined $subspecies;
	
	# If the search string specifies a species, then we look for that
	# name.  We add terms as appropriate to select the specified genus and
	# subgenus if any.
	
	if ( defined $species )
	{
	    push @filter_list, 's.taxon_name like ?';
	    push @param_list, defined $subspecies ? "$species $subspecies"
					 : $species;
	    
	    # If we have a subgenus (and thus also a genus) then we match on
	    # both of them.
	    
	    if ( defined $subgenus )
	    {
		if ( $subgenus ne '%' )
		{
		    push @filter_list, 's.genus like ?';
		    push @param_list, $genus;
		}
		if ( $genus ne '%' )
		{
		    push @filter_list, 's.subgenus like ?';
		    push @param_list, $subgenus;
		}
	    }
	    
	    # If the genus was just specified as '%', no clause is needed
	    # because we want to find all matching species names no matter
	    # what the genus.
	    
	    elsif ( $genus eq '%' )
	    {
	    }
	    
	    # Otherwise, we match the specified genus against both the "genus"
	    # and "subgenus" fields.  This is because the user may not know
	    # whether the genus they are specifying is actually a subgenus.
	    
	    else
	    {
		push @filter_list, '(s.genus like ? or s.subgenus like ?)';
		push @param_list, $genus, $genus;
	    }
	}
	
	# If we were handed a subgenus name (but not a species name) then
	# search for it.
	
	elsif ( defined $subgenus )
	{
	    push @filter_list, 's.taxon_name like ?';
	    push @param_list, $subgenus;
	    push @filter_list, 's.genus like ?';
	    push @param_list, $genus;
	    push @filter_list, "a.taxon_rank = 'subgenus'";
	}
	
	# Otherwise, we were given a single name, so we just look for it in
	# the search_name field.  Of course, in this case, we should exclude
	# species and subspecies matches.
	
	else
	{
	    push @filter_list, 's.taxon_name like ?';
	    push @param_list, $taxon_name;
	    push @filter_list, "a.taxon_rank not in ('species', 'subspecies')";
	}
    }
    
    # If we couldn't parse the name, return empty.
    
    else
    {
	return;
    }
    
    # If the option 'common' was specified, we also need to look in the
    # common_name field.
    
    if ( $options->{common} )
    {
	my $clause = '((' . join(' and ', @filter_list) . ') or a.common_name like ?)';
	@filter_list = ($clause);
	push @param_list, $taxon_name;
    }
    
    # Now, set the other filter parameters, plus query fields and tables,
    # based on the specified options.
    
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
	push @filter_list, $self->generateRankFilter('a', $options->{rank});
    }
    
    if ( defined $options->{kingdom} )
    {
	my $label = $KINGDOM_ALIAS{lc $options->{kingdom}};
	croak "invalid value '$options->{kingdom}' for option 'kingdom'" unless defined $label;
	
	push @filter_list, "t.kingdom = '$label'";
    }
    
    my $query_fields = $AUTH_BASIC_FIELDS;
    my $extra_tables = {};
    
    if ( defined $options->{fields} and not $options->{id} )
    {
	($query_fields, $extra_tables) = $self->generateQueryFields($options->{fields});
    }
    
    if ( defined $options->{order} and $options->{order} =~ /^size/ )
    {
	$query_fields .= ", $LFT_FIELDS" unless $query_fields =~ /t\.lft/;
    }
    
    my $order_expr = '';
    
    if ( defined $options->{order} )
    {
	my $direction = $options->{order} =~ /\.desc$/ ? 'DESC' : 'ASC';
		
	if ( $options->{order} =~ /^size/ )
	{
	    $order_expr = "ORDER BY t.rgt-t.lft $direction";
	}
	
	elsif ( $options->{order} =~ /^name/ )
	{
	    $order_expr = "ORDER BY a.taxon_name $direction";
	}
	
	elsif ( $options->{order} =~ /^lft/ )
	{
	    $order_expr = "ORDER BY t.lft $direction";
	}
	
	else
	{
	    croak "invalid value '$options->{order}' for option 'order'";
	}
    }
    
    my $filter_expr = join(' and ', @filter_list);
    my $extra_joins = $self->generateExtraJoins('a', $extra_tables, $select);
    
    # If we are asked to return only taxon_no values, just do that.
    
    if ( $options->{id} )
    {
	return $self->getTaxaIdsByName($filter_expr, \@param_list, $select, $order_expr, $extra_joins);
    }
    
    # Otherwise prepare and execute the necessary SQL statement, and return the
    # resulting list.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $search_table = $self->{search_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    if ( $select eq 'all' )
    {
	$order_expr = 'ORDER BY if(a.taxon_no = t.spelling_no, 0, 1)'
	    if $order_expr eq '';
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $search_table as s JOIN $auth_table as a using (taxon_no)
			JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table o using (opinion_no)
			$extra_joins
		WHERE $filter_expr
		$order_expr";
    }
    else
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $search_table as s JOIN $auth_table as a2 using (taxon_no)
			JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a ON a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table o using (opinion_no)
			$extra_joins
		WHERE $filter_expr
		GROUP BY a.orig_no $order_expr";
    }
    
    my $result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} }, @param_list);
    
    # If nothing was found, return empty.
    
    unless ( ref $result_list eq 'ARRAY' )
    {
	return {} if $options->{hash};
	return; # otherwise
    }
    
    # Otherwise, bless all of the objects that we found (if any) into the
    # proper package.  If the option 'hash' was specified, we construct a hash
    # reference.  Otherwise, if we were called from a scalar context, return
    # the first element of the list.  Otherwise, we return the whole list.
    
    my %hashref;
    
    foreach my $t (@$result_list)
    {
	bless $t, "Taxon";
	$hashref{$t->{taxon_no}} = $t if $options->{hash};
    }
    
    return \%hashref if $options->{hash};
    return @$result_list if wantarray;
    return $result_list->[0]; # otherwise
}


sub getTaxaIdsByName {

    my ($self, $filter_expr, $param_list, $select, $order_expr) = @_;
    
    # Prepare to fetch the requested information.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $search_table = $self->{search_table};
    my $opinion_table = $self->{opinion_table};
    
    if ( $select eq 'all' )
    {
	$order_expr = 'ORDER BY if(a.taxon_no = t.spelling_no, 0, 1)'
	    if $order_expr eq '';
	
	$SQL_STRING = "
		SELECT a.taxon_no
		FROM $search_table as s JOIN $auth_table as a using (taxon_no)
			JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table o using (opinion_no)
		WHERE $filter_expr
		$order_expr";
    }
    else
    {
	$SQL_STRING = "
		SELECT distinct t.{$select}_no
		FROM $search_table as s JOIN $auth_table as a using (taxon_no)
			JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table o using (opinion_no)
		WHERE $filter_expr
		$order_expr";
    }
    
    # Execute the SQL statement and return the result list (if there is one).
    
    my $result_list = $dbh->selectcol_arrayref($SQL_STRING, {}, @$param_list);
    
    if ( ref $result_list eq 'ARRAY' )
    {
	return @$result_list;
    }
    
    else
    {
	return;
    }
}


=head3 getTaxaBestMatch ( criteria, options )

Returns a list of hashes representing those taxa whose names best match the
specified criteria.  The criteria may be given either as a hash reference with
the following fields, or as a list reference with six values in the following
order:

=over 4

=item genus_reso

=item genus_name

=item subgenus_reso

=item subgenus_name

=item species_reso

=item species_name

=back

Options include:

=over 4

=item id

If specified, returns taxon identifiers instead of Taxon objects.

=back

=cut

sub getTaxaBestMatch {

    my ($self, $params, $options) = @_;
    
    # Start by parsing the parameters, which can be either a direct list or a
    # hash ref.
    
    my ($genus_reso, $genus_name, $subgenus_reso, $subgenus_name, $species_reso, $species_name);
    
    if ( ref $params eq 'HASH' )
    {
        $genus_reso    = $params->{'genus_reso'} || "";
        $genus_name    = $params->{'genus_name'} || "";
        $subgenus_reso = $params->{'subgenus_reso'} || "";
        $subgenus_name = $params->{'subgenus_name'} || "";
        $species_reso  = $params->{'species_reso'} || "";
        $species_name  = $params->{'species_name'} || "";
    }
    
    elsif ( ref $params eq 'ARRAY' )
    {
        ($genus_reso,$genus_name,$subgenus_reso,$subgenus_name,$species_reso,$species_name) = @$params;
    }
    
    else
    {
	croak "parameter must be either a hash ref or an array ref";
    }
    
    # Set option defaults
    
    $options ||= {};
    
    # If $genus_reso is 'informal', or if $genus_name is empty, then we know
    # right away that there won't be any matches.
    
    return if $genus_reso =~ /informal/;
    return if $genus_name eq '';
    
    # A value of 'informal' should be treated the same as an empty value.  A
    # species name that includes a period (such as 'sp.' or 'indet.') or
    # any other non-letter character doesn't count either.
    
    $subgenus_name = '' if $subgenus_reso =~ /informal/;
    $species_name = '' if $species_reso =~ /informal/ or $species_name !~ /^[a-z]+$/;
    
    # Now prepare to fetch the necessary information.
    
    my $dbh = $self->{dbh};
    my $auth_table = $self->{auth_table};
    my $search_table = $self->{search_table};
    my ($genus_match, @genus_list);
    my (@filter_list, @param_list);
    
    my $dbt;
    
    # We match genus against both genus and subgenus, and subgenus against
    # both genus and subgenus, because genera and subgenera are often promoted
    # or demoted.  What was a genus when the name was entered might now be a
    # subgenus, or vice versa.
    
    if ( $subgenus_reso !~ /informal/ and $subgenus_name ne '' )
    {
	$genus_match = "in (?, ?)";
	@genus_list = ($genus_name, $subgenus_name);
    }
    
    else
    {
	$genus_match = "= ?";
	@genus_list = ($genus_name);
    }
    
    # If a species name was specified, we match on that.
    
    if ( $species_name ne '' )
    {
	push @filter_list, "(s.taxon_name = ? and (s.genus $genus_match or s.subgenus $genus_match))";
	push @param_list, $species_name, @genus_list, @genus_list;
    }
    
    # We also look for genera and sub-genera that match.
    
    push @filter_list, "(s.taxon_name $genus_match and a.taxon_rank in ('genus', 'subgenus'))";
    push @param_list, @genus_list;
    
    # Now prepare and execute the query.  See ../scripts/stored_procedure.pl
    # for the definition of 'compute_taxon_match'.
    
    my $filter_expr = join(' or ', @filter_list);
    
    my $SQL_STRING = "
		SELECT a.taxon_no, a.taxon_name, a.orig_no, 
			compute_taxon_match(a.taxon_name, ?, ?, ?) as match_level
		FROM $auth_table as a JOIN $search_table as s using (taxon_no)
		WHERE $filter_expr
		HAVING match_level > 0
		ORDER BY match_level DESC";
    
    my $matches = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} }, 
					   $genus_name, $subgenus_name, $species_name,
					   @param_list);
    
    return unless defined $matches and @$matches > 0;
    
    # If the user requests a array, then return all matches that are in the same class.  The classes are
    #  30: exact match, no need to return any others
    #  20-29: species level match
    #  10-19: genus level match
    
    if (wantarray)
    {
	my $best_match_class = int($matches->[0]{match_level}/10);
	my @matches_in_class;
	foreach my $row (@$matches) {
	    my $match_class = int($row->{match_level}/10);
	    if ($match_class >= $best_match_class) {
		push @matches_in_class, $row;
	    }
	}
	if ( $options->{id} )
	{
	    return map { $_->{taxon_no} } @matches_in_class;
	}
	else
	{
	    return @matches_in_class;
	}
    }

    # If the user requests a scalar, only return the best match.  If the top
    # two are homonyms, we need to check whether they are in the same concept.
    
    elsif ( scalar(@$matches) > 1 and $matches->[0]{taxon_name} eq $matches->[1]{taxon_name} )
    {
	# If the top two matches are in different concepts, return nothing.
	if ( $matches->[0]{orig_no} != $matches->[1]{orig_no} )
	{
	    return;
	}
	# If the top two matches are both in the same concept, return the
	# first if it's the original and the second otherwise.
	elsif ( $matches->[1]{taxon_no} == $matches->[1]{orig_no} )
	{
	    return $options->{id} ? $matches->[1]{taxon_no} : $matches->[1];
	}
	else
	{
	    return $options->{id} ? $matches->[0]{taxon_no} : $matches->[0];
	}
    }
    
    # Otherwise, just return the top match.
    
    else
    {
	return $options->{id} ? $matches->[0]{taxon_no} : $matches->[0];
    }
}


=head3 getTaxaByReference ( reference_no, options )

Returns a list of objects of class C<Taxon> representing all taxa associated
with the given reference.  If no matching taxa are found, returns the empty
list.

If the option 'id' is specified, returns a list of taxon identifiers
instead.  Options include those specified above, plus:

=over 4

=item basis

This option determines how the matching taxonomic concepts are found.  The
possible values are as follows:

=over 4

=item authorities

This is the default, and selects all entries in the C<authorities> table which
are directly associated with the specified reference.

=item opinions

Selects all entries in the C<authorities> table which are associated with
opinions that are in turn associated with the specified reference.

=back

=item rank

Only return taxa of the specified rank.  Examples: family, genus.

=item select

This option determines how taxonomic concepts are treated.  The possible
values are as follows:

=over 4

=item exact

This is the default, and returns one Taxon object for each concept associated
with the specified reference, representing the spelling actually associated
with that reference.

=item spelling

Returns one Taxon object for each concept associated with the specified
reference, representing the currently accepted spelling of that taxon (even if
that is not the spelling associated with the reference).

=item orig

Returns one Taxon object for each concept which has a matching spelling,
representing the original spelling of that concept (even if that is not the
spelling associated with the reference).

=back

=back

=cut

sub getTaxaByReference {

    my ($self, $reference_no, $options) = @_;
    
    # Check the arguments.  Carp a warning and return the empthy list if
    # reference_no is not defined or is zero.
    
    unless ( defined $reference_no and $reference_no > 0 )
    {
	carp "reference_no is undefined or 0";
	return;
    }
    
    # Turn the parameter into a number
    
    $reference_no = $reference_no + 0;
    
    # Set option defaults.
    
    $options ||= {};
    
    my $select = defined $options->{select} ? lc $options->{select} : 'exact';
    
    unless ( $select eq 'exact' or $select eq 'spelling' 
	     or $select eq 'orig' or $select eq 'trad' )
    {
	croak "invalid value '$options->{select}' for option 'select'";
    }
    
    $select = 'trad' if $options->{trad} and $select eq 'spelling';
    
    my $basis = defined $options->{basis} ? lc $options->{basis} : 
	defined $options->{child} ? 'opinions' : 'authorities';
    
    # Set filter parameters and query fields based on the specified
    # options.
    
    my (@filter_list, @param_list);
    
    if ( $basis eq 'authorities' )
    {
	push @filter_list, $select eq 'exact' ? "a.reference_no = ?" : "a2.reference_no = ?";
	push @param_list, $reference_no;
    }
    
    elsif ( $basis eq 'opinions' )
    {
	push @filter_list, "o.reference_no = ?", "o.ref_has_opinion='yes'";
	push @param_list, $reference_no;
    }
    
    else
    {
	croak "invalid basis '$basis'";
    }
    
    my $query_fields = $AUTH_BASIC_FIELDS;
    my $extra_tables = {};
    
    if ( defined $options->{rank} )
    {
	push @filter_list, $self->generateRankFilter('a', $options->{rank});
    }
    
    if ( defined $options->{fields} and not $options->{id} )
    {
	($query_fields, $extra_tables) = $self->generateQueryFields($options->{fields});
	delete $extra_tables->{ref};
    }
    
    my $filter_expr = join(' and ', @filter_list);
    my $extra_joins = $self->generateExtraJoins('a', $extra_tables, $select);
    
    # If we are asked to return only taxon_no values, just do that.
    
    if ( $options->{id} )
    {
	return $self->getTaxaIdsByReference($filter_expr, \@param_list, $basis, $select, '', $extra_joins);
    }
    
    # Otherwise prepare the necessary SQL statement.  We don't need to worry
    # about $query_tables, since currently the only possibiliy is the refs
    # table and we are already joining to that.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    if ( $basis eq 'authorities' and $select eq 'exact' )
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			LEFT JOIN refs as r using (reference_no)
			LEFT JOIN opinions as o on t.opinion_no = o.opinion_no
			$extra_joins
		WHERE $filter_expr";
    }
    
    elsif ( $basis eq 'authorities' )
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN refs as r on a.reference_no = r.reference_no
			LEFT JOIN opinions as o on t.opinion_no = o.opinion_no
			$extra_joins
		WHERE $filter_expr";
    }
    
    elsif ( $basis eq 'opinions' and $select eq 'exact' )
    {
	$query_fields =~ s/o\.status/if(a.taxon_no=o.child_spelling_no,o.status,null) as status/;
	$query_fields .= ', if(a.taxon_no=o.child_spelling_no,o.parent_spelling_no,null) as parent_spelling_no';
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			JOIN opinions as o on
				(a.taxon_no = o.child_spelling_no or a.taxon_no = o.parent_spelling_no)
			LEFT JOIN refs as r on (a.reference_no = r.reference_no)
			$extra_joins
		WHERE $filter_expr";
    }
    
    elsif ( $basis eq 'opinions' )
    {
	$query_fields =~ s/o\.status/if(a.taxon_no=o.child_spelling_no,o.status,null) as status/;
	$query_fields .= ', if(a.taxon_no=o.child_spelling_no,o.parent_spelling_no,null) as parent_spelling_no';
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			JOIN opinions as o on
				(a2.taxon_no = o.child_spelling_no or a2.taxon_no = o.parent_spelling_no)
			LEFT JOIN refs as r on (a.reference_no = r.reference_no)
			$extra_joins
		WHERE $filter_expr";
    }
    
    else
    {
	croak "invalid basis '$basis'";
    }
    
    # Now execute the statement, and return the resulting list if any.
    
    my $result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} }, @param_list);
    
    # If we didn't find anything, return empty.
    
    unless ( ref $result_list eq 'ARRAY' )
    {
	return {} if $options->{hash};
	return; # otherwise
    }
    
    # Otherwise, bless all of the objects that we found (if any) into the
    # proper package.  If the option 'hash' was specified, we construct a hash
    # reference.  Otherwise, we return the list.
    
    my %hashref;
    
    foreach my $t (@$result_list)
    {
	bless $t, "Taxon";
	$hashref{$t->{taxon_no}} = $t if $options->{hash};
    }
    
    return \%hashref if $options->{hash};
    return @$result_list; # otherwise
}


sub getTaxaIdsByReference {

    my ($self, $filter_expr, $param_list, $basis, $select) = @_;
    
    # Prepare to fetch the requested information.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $search_table = $self->{search_table};
    my $opinion_table = $self->{opinion_table};
    
    if ( $basis eq 'authorities' and $select ne 'exact' )
    {
	$SQL_STRING = "
		SELECT distinct t.${select}_no
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
		WHERE $filter_expr";
    }
    
    elsif ( $basis eq 'authorities' )
    {
	$SQL_STRING = "
		SELECT a.taxon_no
		FROM $auth_table as a
		WHERE $filter_expr";
    }
    
    elsif ( $basis eq 'opinions' )
    {
	$SQL_STRING = "
		SELECT a.taxon_no
		FROM $auth_table as a JOIN $opinion_table as o on
				(a.taxon_no = o.child_spelling_no or a.taxon_no = o.parent_spelling_no)
		WHERE $filter_expr";
    }

    else
    {
	croak "invalid basis '$basis'";
    }
    
    # Execute the SQL statement and return the result list (if there is one).
    
    my $result_list = $dbh->selectcol_arrayref($SQL_STRING, {}, @$param_list);
    
    if ( ref $result_list eq 'ARRAY' )
    {
	return @$result_list;
    }
    
    else
    {
	return;
    }
}


=head3 getTaxaByOpinions ( base_taxa, relationship, options )

Returns a list of objects of class C<Taxon> representing all taxa under which
(or over which) any of the given taxa have ever been classified.  The entire
set of opinions is searched to determine this result.  If no matching taxa are
found, returns the empty list.

Possible relationships include:

=over 4

=item seniors

Returns taxa which have been classified as parents of the specified base
taxon. 

=item juniors

Returns taxa which have been classified as children of the specified base
taxon.

Options include those specified above, plus:

=over 4

=item rank

Only return taxa of the specified rank(s).  Examples: family, genus.

=item status

Only return taxa for which the classification has the specified 'status'
value.  The default is 'belongs to'.  Accepted values include:

=over 4

=item valid

Only return taxa which are classified as valid, in other words those for which
the classification status is 'belongs to'.

=item junior

Only return taxa which are classified as junior synonyms, i.e. those for which
the classification status is 'synonym of' or 'replaced by'.

=item invalid

Only return taxa which are classified as invalid, in other words those for
which the classification status is 'invalid', 'nomen dubium', etc.

=item all

Return all classifications regardless of status.

=back

=item select

This option determines how taxonomic concepts are treated.  The possible
values are as follows:

=over 4

=item exact

This is the default, and returns one Taxon object for each taxon found,
representing the spelling actually associated with the opinion which records
the classification.

=item spelling

Returns one Taxon object for each concept found, representing the currently
accepted spelling of that taxon (even if that is not the spelling associated
with the opinion).

=item orig

Returns one Taxon object for each concept found, representing the original
spelling of that concept (even if that is not the spelling associated with the
opinion).

=back

=back

=cut

sub getTaxaByOpinions {

    my ($self, $base_taxa, $parameter, $options) = @_;
    
    # Check arguments.  Only throw an error if we were given a reference but
    # couldn't find a taxon number.  If we were passed the undefined value or
    # a taxon number of zero, carp a warning and return undefined.
    
    my ($base_no, %base_nos);
    
    if ( ref $base_taxa eq 'ARRAY' )
    {
	foreach my $t (@$base_taxa)
	{
	    if ( ref $t )
	    {
		unless ( exists $t->{taxon_no} || exists $t->{orig_no} )
		{
		    carp "could not determine taxon_no";
		    next;
		}
		
		if ( defined $t->{taxon_no} and $t->{taxon_no} > 0 )
		{
		    $base_nos{$t->{taxon_no}} = 1;
		}
		
		elsif ( defined $t->{orig_no} and $t->{orig_no} > 0 )
		{
		    $base_nos{$t->{orig_no}} = 1;
		}
	    }
	    
	    elsif ( $t =~ /^[0-9]+$/ )
	    {
		$base_nos{$t} = 1;
	    }
	}
	
	unless ( keys %base_nos )
	{
	    carp "no valid base taxa were specified";
	    return;
	}
    }
    
    elsif ( ref $base_taxa eq 'HASH' and not exists $base_taxa->{taxon_no} )
    {
	foreach my $t (keys %$base_taxa)
	{
	    if ( $t =~ /^[0-9]+$/ )
	    {
		$base_nos{$t} = 1;
	    }
	}
    }
    
    elsif ( ref $base_taxa )
    {
	croak "could not determine taxon_no from base_taxa" unless
	    exists $base_taxa->{taxon_no} || exists $base_taxa->{orig_no};
	
	$base_no = $base_taxa->{taxon_no} if defined $base_taxa->{taxon_no};
	$base_no = $base_taxa->{orig_no} if defined $base_taxa->{orig_no}
	    and not $base_no > 0;
    }
    
    elsif ( defined $base_taxa && $base_taxa =~ /^[0-9]+$/ )
    {
	$base_no = $base_taxa;
    }
    
    else
    {
	carp "base taxon is undefined or zero";
	return;
    }
    
    my $rel = lc $parameter;
    
    # Set option defaults.
    
    $options ||= {};
    
    my $status = defined $options->{status} && $options->{status} ne '' ? 
	lc $options->{status} : 'valid';
    my $select = defined $options->{select} ? lc $options->{select} : 'spelling';
    
    unless ( $select eq 'spelling' or $select eq 'orig' or $select eq 'trad' )
    {
	croak "invalid value '$options->{select}' for option 'select'";
    }
    
    $select = 'trad' if $options->{trad} and $select eq 'spelling';
    
    # Set filter parameters and query fields based on the specified
    # options.
    
    my (@filter_list);
    
    my $query_fields = $AUTH_BASIC_FIELDS;
    my $extra_tables = {};
    
    if ( $base_no > 0 )
    {
	push @filter_list, 'a2.taxon_no = ' . ($base_no + 0);
    }
    
    else
    {
	push @filter_list, 'a2.taxon_no in (' . join(',', keys %base_nos) . ')';
    }
    
    if ( $status eq 'valid' )
    {
	push @filter_list, "o.status = 'belongs to'";
    }
    elsif ( $status eq 'synonyms' )
    {
	push @filter_list, "o.status in ('belongs to', 'subjective synonym of', 'objective synonym of')";
    }
    elsif ( $status eq 'junior' )
    {
	push @filter_list, "o.status in ('subjective synonym of', 'objective synonym of', 'replaced by')";
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
	push @filter_list, $self->generateRankFilter('a', $options->{rank});
    }
    
    if ( defined $options->{fields} and not $options->{id} )
    {
	($query_fields, $extra_tables) = $self->generateQueryFields($options->{fields});
    }
    
    my $filter_expr = join(' and ', @filter_list);
    $filter_expr = "WHERE $filter_expr" if $filter_expr ne '';
    my $extra_joins = $self->generateExtraJoins('a', $extra_tables, $select);
    
    # If we were asked for just the taxon_no, do that.
    
    if ( $options->{id} )
    {
	return $self->getClassificationIds($rel, $select, $filter_expr, $extra_joins);
    }
    
    # Prepare the necessary SQL statement.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $search_table = $self->{search_table};
    my $opinion_table = $self->{opinion_table};
    
    my ($anchor, $variable, $result_list);
    
    if ( $rel eq 'seniors' )
    {
	$anchor = 'child';
	$variable = 'parent';
    }
    
    elsif ( $rel eq 'juniors' )
    {
	$anchor = 'parent';
	$variable = 'child';
    }
    
    else
    {
	croak "invalid relationship '$rel'";
    }
    
    if ( $select eq 'exact' )
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			JOIN opinions as o on a.taxon_no = o.${variable}_spelling_no
			JOIN $auth_table as a2 on a2.orig_no = o.${anchor}_no
			$extra_joins
		$filter_expr";
    }
    
    else
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a JOIN $tree_table as t on a.taxon_no = t.${select}_no
			JOIN opinions as o on t.orig_no = o.${variable}_no
			JOIN $auth_table as a2 on a2.orig_no = o.${anchor}_no
			$extra_joins
		$filter_expr";
    }
    
    # Execute the SQL statement and return the result list (if there is one).
    
    my $result_list = $dbh->selectall_arrayref($SQL_STRING, {});
    
    # If we didn't find any results, return nothing.
    
    unless ( ref $result_list eq 'ARRAY' )
    {
	return {} if $options->{hash};
	return; # otherwise
    }
    
    # Otherwise, bless all of the objects that we found (if any) into the
    # proper package.  If the option 'hash' was specified, we construct a hash
    # reference.  Otherwise, we return the list.
    
    my %hashref;
    
    foreach my $t (@$result_list)
    {
	bless $t, "Taxon";
	$hashref{$t->{taxon_no}} = $t if $options->{hash};
    }
    
    return \%hashref if $options->{hash};
    return @$result_list; # otherwise
}


sub getClassificationIds {

    my ($self, $rel, $select, $filter_expr, $extra_joins) = @_;
    
    # Prepare to fetch the requested information.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    my ($anchor, $variable);
    
    if ( $rel eq 'seniors' )
    {
	$anchor = 'child';
	$variable = 'parent';
    }
    
    elsif ( $rel eq 'juniors' )
    {
	$anchor = 'parent';
	$variable = 'child';
    }
    
    else
    {
	croak "invalid relationship '$rel'";
    }
    
    # Generate and execute the necessary SQL statement
    
    if ( $select eq 'exact' )
    {
	$SQL_STRING = "
		SELECT a.taxon_no
		FROM $auth_table as a JOIN $opinion_table as o on a.taxon_no = o.${variable}_spelling_no
			JOIN $auth_table as a2 on a2.orig_no = o.${anchor}_no
			$extra_joins
		$filter_expr";
    }
    
    else
    {
	$SQL_STRING = "
		SELECT a.taxon_no
		FROM $auth_table as a JOIN $tree_table as t on a.taxon_no = t.${select}_no
			JOIN $opinion_table as o on t.orig_no = o.${variable}_no
			JOIN $auth_table as a2 on a2.orig_no = o.${anchor}_no
			$extra_joins
		$filter_expr";
    }
    
    my $taxon_nos = $dbh->selectcol_arrayref($SQL_STRING);
    
    # If we got some results, return them.  Otherwise, return the empty list.
    
    if ( ref $taxon_nos eq 'ARRAY' )
    {
	return @$taxon_nos;
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

=item senior

Returns an object representing the senior synonym of the base taxon.

=item parent

Returns an object representing the parent taxon of the base taxon.

=item classification

Returns an object representing the taxon under which the base taxon is
classified.  This will be either the immediate parent or immediate senior
synonym of the base taxon, depending upon its classification opinion.

=back

Available options include:

=over 4

=item select

This option determines how taxonomic concepts are treated.  It is ignored for
relationships 'orig' and 'spelling'.  The possible values are as follows:

=over 4

=item spelling

This is the default, and causes this routine to return the currently accepted
spelling of the indicated taxonomic concept.

=item orig

Causes this routine to return the original spelling of the indicated taxonomic concept.

=back

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
	
	$base_no = $base_taxon->{taxon_no} + 0 if defined $base_taxon->{taxon_no}
	    and $base_taxon->{taxon_no} > 0;
	$base_no = $base_taxon->{orig_no} if defined $base_taxon->{orig_no}
	    and $base_taxon->{orig_no} > 0 and not $base_no > 0;
    }
    
    if ( defined $base_taxon and $base_taxon =~ /^[0-9]+$/ and not $base_no > 0 )
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
    
    my ($taxon, $select);
    
    $options ||= {};
    
    my $select = defined $options->{select} ? lc $options->{select} : 'spelling';
    
    unless ( $select eq 'spelling' or $select eq 'orig' or $select eq 'trad' )
    {
	croak "invalid value '$options->{select}' for option 'select'";
    }
    
    $select = 'trad' if $options->{trad} and $select eq 'spelling';
    
    my $query_fields = $AUTH_BASIC_FIELDS;
    my $extra_tables = {};
    
    if ( defined $options->{fields} and not $options->{id} )
    {
	($query_fields, $extra_tables) = $self->generateQueryFields($options->{fields});
    }
    
    my $extra_joins = $self->generateExtraJoins('a', $extra_tables, $select);
    
    # The relationship 'senior' selects the field 'synonym_no'.
    
    $rel = 'synonym' if $rel eq 'senior';
    
    # If we were asked for just the taxon_no, do that.
    
    if ( $options->{id} )
    {
	return $self->getRelatedTaxonId($base_no, $rel, $select);
    }
    
    # Otherwise, prepare and execute the relevant SQL query
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    # Parameter self is quite easy to evaluate
    
    if ( $rel eq 'self' )
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		WHERE a.taxon_no = ?";
    }
    
    # Parameters orig_no and spelling_no require a simple look-up
    
    elsif ( $rel eq 'orig' or $rel eq 'spelling' or $rel eq 'trad' )
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on a.taxon_no = t.${rel}_no
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		WHERE a2.taxon_no = ?";
    }
    
    # Parameters synonym_no and parent_no require an extra join on $tree_table
    # To look up the current spelling.
    
    elsif ( $rel eq 'synonym' or $rel eq 'parent' )
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.orig_no = t2.${rel}_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		WHERE a2.taxon_no = ?";
    }
    
    # Parameter 'classification' requires an additional use of $opinion_table as well
    
    elsif ( $rel eq 'classification' )
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $opinion_table as o2 using (opinion_no)
			JOIN $tree_table as t on t.orig_no = o2.parent_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		WHERE a2.taxon_no = ?";
    }
    
    else
    {
	croak "invalid relationship '$rel'";
    }
    
    my $taxon = $dbh->selectrow_hashref($SQL_STRING, undef, $base_no);
    
    # If we found a taxon, return it.  If the option 'hash' was specified,
    # then return a hashref instead.
    
    if ( defined $taxon )
    {
	bless $taxon, "Taxon";
	
	return { $taxon->{taxon_no} => $taxon } if $options->{hash};
	return $taxon; # otherwise
    }
    
    # Otherwise, return empty.
    
    else
    {
	return {} if $options->{hash};
	return; # otherwise
    }
}


sub getRelatedTaxonId {
    
    my ($self, $base_no, $rel, $select) = @_;
    
    # Prepare to fetch the requested information.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    # Parameters orig_no and spelling_no require a simple lookup.
    
    if ( $rel eq 'orig' or $rel eq 'spelling' or $rel eq 'trad' )
    {
	$SQL_STRING = "
		SELECT t.${rel}_no
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
		WHERE a2.taxon_no = ?";
    }
    
    # Parameters synonym_no and parent_no require an extra join on $tree_table
    # to look up the spelling_no.
    
    elsif ( $rel eq 'synonym' or $rel eq 'parent' )
    {
	$SQL_STRING = "
		SELECT t.${select}_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.orig_no = t2.${rel}_no
		WHERE a2.taxon_no = ?";
    }
    
    # Parameter 'classification' requires an additional use of $opinion_table
    # as well
    
    elsif ( $rel eq 'classification' )
    {
	$SQL_STRING = "
		SELECT t.${select}_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $opinion_table o using (opinion_no)
			JOIN $tree_table as t on t.orig_no = o.parent_no
		WHERE a2.taxon_no = ?";
    }
    
    else
    {
	croak "invalid relationship '$rel'";
    }
    
    my ($taxon_no) = $dbh->selectrow_array($SQL_STRING, undef, $base_no);

    # Return the taxon number, or undefined if none was found.
    
    return $taxon_no;
}


=head3 getRelatedTaxa ( base_taxa, relationship, options )

Returns a list of Taxon objects having the specified relationship to the
specified base taxon or taxa.  If no matching taxa are found, returns an empty
list.  The parameter C<base_taxa> may be either a taxon number or a Taxon
object, an array of either of these, or a hash whose keys are taxon numbers.

In addition to the basic fields that are returned by all of the methods of
this class, the field C<base_no> will be included where appropriate to
indicate which of the base taxa each returned taxon is related to.

Other tables can be joined to the query, by means of the options
'join_tables', 'extra_fields' and 'extra_filters'.  This is perhaps the most
flexible and useful of the methods provided by this class.

Possible relationships are:

=over 4

=item self

Returns a list of objects representing the specified base taxa.

=item spellings

Returns a list of objects representing the various spellings of the base
taxa.

=item current

Returns a list of objects representing the current spellings of the base taxa.

=item originals

Returns a list of objects representing the original spellings of the base
taxa.

=item synonyms

Returns a list of objects representing the various synonyms of the base taxa.

=item seniors

Returns a list of objects representing the senior synonyms of the base taxa.

=item classifications

Returns a list of objects representing the taxa under which each of the base
taxa is classified (parent or senior synonym).

=item parents

Returns a list of objects representing the immediate parents of the base taxa.

=item all_parents

Returns a list of objects representing all taxa that contain any of the base
taxa, from the kingdom level on down.

=item children

Returns a list of objects representing the immediate children of the base
taxon or taxa.

=item all_children

Returns a list of objects representing all of the descendants of the base
taxon or taxa (all of the taxa contained within the given taxa).

=item all_taxa

Returns a list of all taxa in the database.  This keyword would typically be
used rarely, as it will return several tens of megabytes of data unless other
constraining options (i.e. pubyr, author, reference_no) are included as well.

=back

Possible options are:

=over 4 

=item rank

This option filters the list of resulting taxa, returning only those that
match the given rank.  The value can be a single rank, or a list.  In the
latter case, each item can be either a single rank, or a list of [min, max].
This option is only valid for relationships 'parent', 'child', 'all_parents',
and 'all_children'.

=item author

This option filters the list of resulting taxa, returning only those that
match the author name given as the value of the option.  The value of this
option should be a last name only, and selects taxa which match in the 'first
author' or 'second author' fields.  The value can either be a single name, a
comma-separated list, or a listref.

=item pubyr

This option filters the list of resulting taxa, returning only those that
match the publication year given as the value of the option.  This match is done
according to the 'pubyr_rel' option if specified, or an exact match otherwise.

=item pubyr_rel

This option affects the way in which the 'pubyr' option is interpreted, and
otherwise has no effect.  Possible values are '<', '<=', '=', '>=', '>', '<>',
'before', 'after', and 'exact'.

=item reference_no

This option filters the list of resulting taxa, returning only those that are
associated with the reference_no given as the value of the option.  The value
can either be a single reference_no, a comma-separated list, or a listref.

=item person_no

This option filters the list of resulting taxa, returning only those that have
been touched by the person or people given by the value of the option.  The
value can either be a single person_no value, a comma-separated list of them,
or a listref.  The filter is done according to the value of the option
'person_rel' if specified, or 'all' if not.

=item person_rel

This option affects the way in which the 'person' option is interpreted, and
otherwise has no effect.  Possible values are 'all', 'authorizer', 'enterer',
'modifier', and 'authorizer_enterer'.  Taxa are selected only if any of the
people specified by the 'person' option have touched them in the role or roles
specified by this option.

=item created

This option filters the list of resulting taxa, returning only those for which
the 'created' date on the record relates to the date specified by the value of
this option according to 'created_rel'.  If the latter option was not
specified, it defaults to 'after'.

=item created_rel

This option affects the way in which the 'created' option is interpreted, and
otherwise has no effect.  Possible values are '<', 'before', '>=', 'after'.

=item modified

This option filters the list of resulting taxa, returning only those for which
the 'modified' date on the record relates to the date specified by the value
of this option according to 'modified_rel'.  If the latter option was not
specified, it defaults to 'after'.

=item modified_rel

This option affects the way in which the 'modified' option is interpreted, and
otherwise has no effect.  Possible values are '<', 'before', '>=', 'after'.

=item status

This option filters the list of resulting taxa according to taxonomic status.
If 'valid', only valid taxa are returned.  If 'invalid', only invalid taxa are
returned.  If 'synonyms', only valid taxa and junior synonyms.  If 'all', all
matching taxa are returned regardless of taxonomic or nomenclatural status.
The default is 'valid' for the relationship 'parent' and 'child', and 'all'
for 'synonym' and 'spelling'.

=item exclude

If specified, then the indicated taxa and all of their children are excluded
from the returned list.  This option is only valid for relationships
'children', 'all_children' and 'all_taxa'.

=item exclude_self

If specified, then the base taxon or taxa are excluded from the returned
list.  Otherwise (the default) they are included.

=item distinct

If this option is specified, only one object will be returned for each
distinct taxon in the result set.  The C<base_no> field will not be included
in any of the returned objects, because it is possible that the same taxon
could have the same relationship (i.e. senior synonym) to more than one member
of the base taxa.

=item hash

If this option is specified, then instead of a list of Taxon objects we get
back a hash whose keys are taxon_nos and whose values are the Taxon objects.
If the value of this option is 'base', then the keys are the base_no values.
Otherwise, the keys are taxon_no values as with the other methods in this
class.

=item select

This option determines how taxonomic concepts are treated.  It is ignored if
the relationship is 'spelling'.  The possible values are as follows:

=over 4

=item spelling

This is the default (except for the relationships 'spelling' and 'originals'),
and causes this routine to return the currently accepted spelling of each of
the matching taxonomic concepts.

=item orig

Causes this routine to return the original spelling of each of the matching
taxonomic concepts.

=item all

Causes this routine to return all matching spellings, or all spellings of the
matching taxonomic concepts.

=back

=item join_tables

This option causes one or more extra tables to be joined to the query.  The
value of this option should be an SQL join clause, such as "LEFT JOIN ecotaph
as qe on taxon_no = [taxon_no]".  The table should be given an alias starting
with the letter 'q', which is guaranteed not to conflict with any of the other
tables used in the query.  The clause must include the string '[taxon_no]'
somewhere inside it, which will be replaced with the name of the column
containing the taxon_no values for the returned taxa.  The option 'select' can
be used to match orig_no or spelling_no values instead.

=item extra_fields

This option is designed to be used with 'join_tables'.  Its value is appended
to the list of fields selected by the resulting query.  For example:
'qe.adult_length, qe.adult_width'.

=item extra_filters

This option is designed to be used with 'join_tables'.  Its value is appended
to the WHERE clause in the resulting query.  For example: "qe.composition1 =
'aragonite'".

=back

=cut
 
sub getRelatedTaxa {
    
    my ($self, $base_taxa, $parameter, $options) = @_;
    
    # Check arguments.  Only throw an error if we were given a reference but
    # couldn't find a taxon number.  If we were passed the undefined value or
    # a taxon number of zero, carp a warning and return undefined.
    
    my $rel = lc $parameter;
    
    my ($base_no, %base_nos);
    
    if ( ref $base_taxa eq 'ARRAY' )
    {
	foreach my $t (@$base_taxa)
	{
	    if ( ref $t )
	    {
		unless ( exists $t->{taxon_no} || exists $t->{orig_no} )
		{
		    carp "could not determine taxon_no";
		    next;
		}
		
		if ( defined $t->{taxon_no} and $t->{taxon_no} > 0 )
		{
		    $base_nos{$t->{taxon_no}} = 1;
		}
		
		elsif ( defined $t->{orig_no} and $t->{orig_no} > 0 )
		{
		    $base_nos{$t->{orig_no}} = 1;
		}
	    }
	    
	    elsif ( $t =~ /^[0-9]+$/ )
	    {
		$base_nos{$t} = 1;
	    }
	}
	
	unless ( keys %base_nos )
	{
	    carp "no valid base taxa were specified";
	    return;
	}
    }
    
    elsif ( ref $base_taxa eq 'HASH' and not exists $base_taxa->{taxon_no} )
    {
	foreach my $t (keys %$base_taxa)
	{
	    if ( $t =~ /^[0-9]+$/ )
	    {
		$base_nos{$t} = 1;
	    }
	}
    }
    
    elsif ( ref $base_taxa )
    {
	croak "could not determine taxon_no from base_taxa" unless
	    exists $base_taxa->{taxon_no} || exists $base_taxa->{orig_no};
	
	$base_no = $base_taxa->{taxon_no} if defined $base_taxa->{taxon_no};
	$base_no = $base_taxa->{orig_no} if defined $base_taxa->{orig_no}
	    and not $base_no > 0;
    }
    
    elsif ( defined $base_taxa && $base_taxa =~ /^[0-9]+$/ )
    {
	$base_no = $base_taxa;
	$base_nos{$base_no} = 1;
    }
    
    elsif ( $rel eq 'all_taxa' )
    {
	$base_no = 0;	# if the relationship is 'all_taxa', we ignore $base_taxa
    }
    
    else
    {
	carp "base taxon is undefined or zero";
	return;
    }
    
    # Set option defaults.
    
    my $query_fields = $AUTH_BASIC_FIELDS;
    my $extra_tables = {};
    
    $options ||= {};
    
    my $status;
    
    if ( defined $options->{status} and $options->{status} ne '' )
    {
	$status = lc $options->{status};
    }
    
    elsif ( $rel eq 'children' or $rel eq 'all_children' or $rel eq 'all_taxa' )
    {
	$status = 'valid';
    }
    
    else
    {
	$status = 'all';
    }
    
    my $select = defined $options->{select} ? lc $options->{select} : 'spelling';
    
    unless ( $select eq 'spelling' or $select eq 'orig' or $select eq 'trad'
	     or ($select eq 'all' and ($rel eq 'all_taxa' or $rel eq 'all_children')) )
    {
	croak "invalid value '$options->{select}' for option 'select'";
    }
    
    $select = 'trad' if $options->{trad} and $select eq 'spelling';
    
    # Set query fields based on the specified options.  This call also tells
    # us which extra tables we will need in order to retrieve those fields.
    # We always include the necessary table to retrieve the parent taxon if
    # the relationship is 'all_parents'.
    
    if ( defined $options->{fields} and not $options->{id} )
    {
	($query_fields, $extra_tables) = $self->generateQueryFields($options->{fields});
    }
    
    if ( defined $options->{order} and $options->{order} =~ /^size/ )
    {
	$query_fields .= ", $LFT_FIELDS" unless $query_fields =~ /t\.lft/;
    }
    
    if ( $rel eq 'all_parents' )
    {
	$extra_tables->{pa} = 1;
    }
    
    # Set filter clauses based on the specified options
    
    my (@filter_list, @param_list);
    
    if ( $rel ne 'all_parents' and $rel ne 'all_taxa' and $base_no > 0 )
    {
	push @filter_list, 'a2.taxon_no = ?';
	push @param_list, $base_no;
    }
    
    elsif ( $rel ne 'all_parents' and $rel ne 'all_taxa' )
    {
	push @filter_list, 'a2.taxon_no in (' . join(',', keys %base_nos) . ')';
    }
    
    if ( $status eq 'valid' )
    {
	push @filter_list, "o.status = 'belongs to'";
    }
    
    elsif ( $status eq 'synonyms' ) {
	push @filter_list, "o.status in ('belongs to', 'subjective synonym of', 'objective synonym of')";
    }
    elsif ( $status eq 'invalid' ) {
	push @filter_list, "o.status not in ('belongs to', 'subjective synonym of', 'objective synonym of')";
    }
    elsif ( $status eq 'all' or $status eq 'any' ) {
	# no filter needed
    }
    else {
	croak "invalid value '$status' for option 'status'";
    }
    
    if ( $options->{exclude_self} )
    {
	if ( $rel eq 'all_parents' )
	{
	    push @filter_list, "s.gen > 1";
	}
	
	elsif ( $rel eq 'all_children' )
	{
	    push @filter_list, "t.lft != t2.lft";
	}
	
	elsif ( $rel eq 'synonyms' or $rel eq 'seniors' )
	{
	    push @filter_list, "t.orig_no != t2.orig_no";
	}
	
	elsif ( $rel eq 'spellings' or $rel eq 'current' )
	{
	    push @filter_list, "a.taxon_no != a2.taxon_no";
	}
    }
    
    if ( defined $options->{exclude} )
    {
	unless ( $rel eq 'children' or $rel eq 'all_children' or $rel eq 'all_taxa' )
	{
	    croak "option 'exclude' is not valid with relationship '$rel'";
	}
	
	my @include_list = ($base_no > 0 ? $base_no : keys(%base_nos));
	
	push @filter_list, $self->generateExcludeFilter('a', $options->{exclude}, \@include_list );
    }
    
    if ( defined $options->{rank} )
    {
	push @filter_list, $self->generateRankFilter('a', $options->{rank});
    }
    
    if ( defined $options->{author} )
    {
	push @filter_list, $self->generateAuthorFilter('a', $options->{author});
	$extra_tables->{ref} = 1;
    }
    
    if ( defined $options->{pubyr} )
    {
	push @filter_list, $self->generatePubyrFilter('a', $options->{pubyr},
						      $options->{pubyr_rel});
	$extra_tables->{ref} = 1;
    }
    
    if ( defined $options->{created} )
    {
	push @filter_list, $self->generateDateFilter('a.created', $options->{created},
						     $options->{created_rel});
    }
    
    if ( defined $options->{modified} )
    {
	push @filter_list, $self->generateDateFilter('a.modified', $options->{modified},
						     $options->{modified_rel});
    }
    
    if ( defined $options->{reference_no} )
    {
	push @filter_list, $self->generateRefnoFilter('a', $options->{reference_no});
    }
    
    if ( defined $options->{person_no} )
    {
	push @filter_list, $self->generatePersonFilter('a', $options->{person_no},
						       $options->{person_rel});
    }
    
    # Select the order in which the results will be returned
    
    my $order_expr = '';
    
    if ( defined $options->{order} )
    {
	my $direction = $options->{order} =~ /\.desc$/ ? 'DESC' : 'ASC';
		
	if ( $options->{order} =~ /^size/ )
	{
	    $order_expr = "ORDER BY t.rgt-t.lft $direction";
	}
	
	elsif ( $options->{order} =~ /^name/ )
	{
	    $order_expr = "ORDER BY a.taxon_name $direction";
	}
	
	else
	{
	    croak "invalid value '$options->{order}' for option 'order'";
	}
    }
    
    # Add any extra fields and filters that were explicitly specified
    
    if ( defined $options->{extra_fields} and
	 $options->{extra_fields} ne '' )
    {
	$query_fields .= ', ' . $options->{extra_fields};
    }
    
    if ( defined $options->{extra_filters} and
	 $options->{extra_filters} ne '' )
    {
	push @filter_list, $options->{extra_filters};
    }
    
    # Add in 'base_no' if appropriate
    
    unless ( $options->{distinct} or $rel eq 'self' or $rel eq 'all_taxa' or $rel eq 'all_parents' )
    {
	$query_fields .= ', a2.taxon_no as base_no';
    }
    
    # Compute the necessary expressions to build the query
    
    my $filter_expr = join(' and ', @filter_list);
    $filter_expr = "WHERE $filter_expr" if $filter_expr ne '';
    my $extra_joins = $self->generateExtraJoins('a', $extra_tables, $select);
    
    # Add any extra joins that were explicitly specified
    
    if ( defined $options->{join_tables} )
    {
	my $join_tables = $options->{join_tables};
	
	$join_tables =~ s/\[taxon_no\]/a.taxon_no/g;
	
	$extra_joins .= $join_tables;
    }
    
    # Now, get ready to do the query.  If we were asked for just the taxon_nos, do
    # that.
    
    if ( $options->{id} )
    {
	my $plist = \@param_list;
	
	if ( $rel eq 'all_parents' )
	{
	    $plist = [keys %base_nos];
	}
	
	return $self->getRelatedTaxaIds($plist, $rel, $select, $filter_expr, $order_expr, 
					$extra_joins);
    }
    
    # Otherwise, prepare to execute the relevant SQL query
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    my ($result_list, $base_list);
    
    # For parameter 'self', we just select the indicated taxa.  The 'distinct'
    # option is irrelevant here, since only one row is selected for each
    # distinct taxon in the base set.
    
    if ( $rel eq 'self' )
    {
	$filter_expr =~ s/a2\./a\./g;
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		$filter_expr";
    }
    
    # For parameter 'spellings', make sure to return the currently accepted
    # spelling(s) first.
    
    elsif ( $rel eq 'spellings' )
    {
	$order_expr = "ORDER BY if(a.taxon_no = t.${select}_no, 0, 1)"
	    if $order_expr eq '';
	
	$order_expr = "GROUP BY a.taxon_no $order_expr" if $options->{distinct};

	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $auth_table as a using (orig_no)
			JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		$filter_expr
		$order_expr";
    }
    
    # For parameter 'originals', we select just the original spellings.
    
    elsif ( $rel eq 'originals' )
    {
	$order_expr = "GROUP BY a.taxon_no $order_expr" if $options->{distinct};
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $auth_table as a on a.taxon_no = a2.orig_no
			JOIN $tree_table as t on a2.orig_no = t.orig_no
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		$filter_expr
		GROUP BY a.taxon_no
		$order_expr";
    }
    
    # For parameter 'current', we select just the current spellings.
    
    elsif ( $rel eq 'current' )
    {
	$order_expr = "GROUP BY a.taxon_no $order_expr" if $options->{distinct};
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		$filter_expr
		GROUP BY a.taxon_no
		$order_expr";
    }
    
    # For parameter 'synonyms', make sure to return the most senior synonym(s)
    # first.
    
    elsif ( $rel eq 'synonyms' )
    {
	$order_expr = 'ORDER BY if(a.orig_no = t.synonym_no, 0, 1)'
	    if $order_expr eq '';
	
	$order_expr = "GROUP BY a.taxon_no $order_expr" if $options->{distinct};
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t using (synonym_no)
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr";
    }
    
    # For parameter 'seniors', we select just the senior synonyms.
    
    elsif ( $rel eq 'seniors' )
    {
	$order_expr = "GROUP BY a.taxon_no $order_expr" if $options->{distinct};
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.orig_no = t2.synonym_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr";
    }
    
    # For parameter 'all_taxa', order the results by tree sequence unless
    # otherwise specified.  We need a slightly different query for
    # select='all' than for the others.  The 'distinct' option is irrelevant
    # here, because only one row will be selected for each distinct taxon in
    # all cases.
    
    elsif ( $rel eq 'all_taxa' )
    {
	my $join_expr = $select eq 'all' ? "using (orig_no)" :
	    "on a.taxon_no = t.${select}_no";
	
	$order_expr = 'ORDER BY t.lft'
	    if $order_expr eq '';
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a JOIN $tree_table as t $join_expr
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr";
    }
    
    # For parameter 'children' or 'all_children', order the results by tree
    # sequence unless otherwise specified.
    
    elsif ( $rel eq 'children' or $rel eq 'all_children' )
    {
	my $level_filter = $rel eq 'children' ? 'and t.depth = t2.depth + 1' : '';
	
	my $join_expr = $select eq 'all' ? "on a.orig_no = t.orig_no" :
	    "on a.taxon_no = t.${select}_no";
	
	$order_expr = 'ORDER BY t.lft'
	    if $order_expr eq '';
	
	$order_expr = "GROUP BY a.taxon_no $order_expr" if $options->{distinct};
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft >= t2.lft and t.lft <= t2.rgt
				$level_filter
			JOIN $auth_table as a $join_expr
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr";
    }
    
    # For parameter 'classifications', do a straightforward lookup using a
    # second copy of the opinion table.
    
    elsif ( $rel eq 'classifications' )
    {
	$order_expr = "GROUP BY a.taxon_no $order_expr" if $options->{distinct};
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $opinion_table as o2 using (opinion_no)
			JOIN $tree_table as t on t.orig_no = o2.parent_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr";
    }

    # For parameter 'parents', do a straightforward lookup.
    
    elsif ( $rel eq 'parents' )
    {
	$order_expr = "GROUP BY a.taxon_no $order_expr" if $options->{distinct};
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.orig_no = t2.parent_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr";
    }
    
    # For parameter 'all_parents', we need a more complicated procedure in order to
    # do the query efficiently.  This requires using a scratch table and a
    # stored procedure to recursively fill it in.  The scratch table cannot
    # be a temporary table, due to a limitation of MySQL, so we need to use a
    # global table with locks :(
    
    # The 'distinct' option is irrelevant here, because the stored procedure
    # 'compute_ancestry' generates only one row for each distinct taxon in the
    # result set.
    
    elsif ( $rel eq 'all_parents' )
    {
	my $result;
	
	my $extra_locks = $self->generateExtraLocks($extra_tables);
	
	$result = $dbh->do("LOCK TABLES $ANCESTRY_SCRATCH WRITE,
				    $ANCESTRY_SCRATCH as s WRITE,
				    $auth_table as a READ,
				    $opinion_table as o READ,
				    $tree_table as t READ
				    $extra_locks");
	
	# We need a try block to make sure that the table locks are released
	# no matter what else happens.
	
	try
	{
	    # Clear the scratch table.  We do this at start instead of at end
	    # because it is unavoidably a global table and it is not easy to
	    # guarantee that it will get cleared in all cases if errors occur.
	    
	    $result = $dbh->do("DELETE FROM $ANCESTRY_SCRATCH");
	    
	    # Seed the scratch table with the starting taxon_no values (one or
	    # more).
	    
	    my @tuples = map { "($_, 1)" } keys %base_nos;
	    
	    $result = $dbh->do("INSERT IGNORE INTO $ANCESTRY_SCRATCH VALUES " .
			       join(',', @tuples));
	    
	    # Now call a stored procedure which iteratively inserts the
	    # parents of the taxa in $ANCESTRY_SCRATCH back into it until the
	    # top of the taxonomic hierarchy is reached.
	    
	    $result = $dbh->do("CALL compute_ancestry(0)");
	    
	    # Finally, we can use this scratch table to get the information we
	    # need.
	    
	    $order_expr = 'ORDER BY t.lft'
		if $order_expr eq '';
	    
	    $SQL_STRING = "
		SELECT $query_fields, pa.taxon_no as parent_taxon_no
		FROM $tree_table as t JOIN $ANCESTRY_SCRATCH as s on s.orig_no = t.orig_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		GROUP BY t.lft
		$order_expr";

	    $result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
	    
	    # If we were asked for a 'base' hash, we also need to include a
	    # record for each original taxon.
	    
	    if ( $options->{hash} eq 'base' )
	    {
		my $select_expr = "WHERE a.taxon_no in (" . join(',', keys %base_nos) . ")";
		
		$base_list = $dbh->selectall_arrayref("
		SELECT $query_fields, pa.taxon_no as parent_taxon_no
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$select_expr
		$order_expr", { Slice => {} });
	    }
	}
	
	finally {
	    $dbh->do("UNLOCK TABLES");
	    die $_[0] if defined $_[0];
	};
	
	# As a final step, we need to go through the list and see if it
	# includes multiple kingdom-level taxa.  If so, we need to delete
	# everything up to the last kingdom-level taxon found.
	
	my $last_kingdom = 0;
	
	if ( ref $result_list eq 'ARRAY' )
	{
	    foreach my $i (0..$#$result_list)
	    {
		$last_kingdom = $i if $result_list->[$i]{taxon_rank} eq 'kingdom';
	    }
	    
	    if ( $last_kingdom > 0 )
	    {
		splice @$result_list, 0, $last_kingdom;
	    }
	}
    }
    
    else
    {
	croak "invalid relationship '$parameter'";
    }
    
    # Except for 'all_parents', in which case the query had to be executed within
    # the scope of the lock statement, we now execute the indicated query.
    
    unless ( $rel eq 'all_parents' )
    {
	$result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} }, @param_list);
    }

    # If we didn't get any results, return nothing.
    
    unless ( ref $result_list eq 'ARRAY' )
    {
	return;
    }
    
    # Otherwise, bless all of the objects that we found (if any) into the
    # proper package.  If the option 'hash' was specified, we construct a hash
    # reference.  Otherwise, we return the list.
    
    my %hashref;
    
    if ( $options->{hash} eq 'base' and $rel eq 'all_parents' )
    {
	foreach my $t (@$base_list, @$result_list)
	{
	    bless $t, 'Taxon';
	    $hashref{$t->{taxon_no}} = $t;
	}
	
	return \%hashref;
    }
    
    elsif ( $options->{hash} eq 'base' )
    {
	foreach my $t (@$result_list)
	{
	    bless $t, 'Taxon';
	    $hashref{$t->{base_no}} ||= [];
	    push @{$hashref{$t->{base_no}}}, $t;
	}
	
	return \%hashref;
    }
    
    elsif ( $options->{hash} )
    {
	foreach my $t (@$result_list)
	{
	    bless $t, 'Taxon';
	    $hashref{$t->{taxon_no}} = $t;
	}
	
	return \%hashref;
    }
    
    else
    {
	foreach my $t (@$result_list)
	{
	    bless $t, 'Taxon';
	}

	return @$result_list;
    }
}


sub getRelatedTaxaIds {
    
    my ($self, $param_list, $rel, $select, $filter_expr, $order_expr, $extra_joins) = @_;
    
    # Prepare to fetch the requested information.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    my ($taxon_nos);
    
    # For parameter 'self', we do a simple lookup.
    
    if ( $rel eq 'self' )
    {
	$filter_expr =~ s/a2\./a\./g;
	
	$SQL_STRING = "
		SELECT a.taxon_no
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		$filter_expr";
    }
    
    # For parameter 'spellings', make sure to return the currently accepted
    # spelling first
    
    elsif ( $rel eq 'spellings' )
    {
	$SQL_STRING = "
		SELECT a.taxon_no
		FROM $auth_table as a2 JOIN $auth_table as a using (orig_no)
			JOIN $tree_table as t on t.orig_no = a.orig_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		ORDER BY if(a.taxon_no = t.${select}_no, 0, 1)";
    }
    
    # For parameter 'originals', we select the original spellings
    
    elsif ( $rel eq 'originals' )
    {
	$SQL_STRING = "
		SELECT DISTINCT a.taxon_no
		FROM $auth_table as a2 JOIN $auth_table as a on a.taxon_no = a2.orig_no
			JOIN $tree_table as t on t.orig_no = a.orig_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		ORDER BY t.lft";
    }
    
    # For parameter 'current', we select the current spellings
    
    elsif ( $rel eq 'current' )
    {
	$SQL_STRING = "
		SELECT DISTINCT a.taxon_no
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		ORDER BY t.lft";
    }
    
    # for parameter 'synonyms', make sure to return the most senior synonym first
    
    elsif ( $rel eq 'synonyms' )
    {
	$SQL_STRING = "
		SELECT t.${select}_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t using (synonym_no)
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		ORDER BY if(t.orig_no = t.synonym_no, 0, 1)";
    }
    
    # For parameter 'seniors', we select the senior synonyms
    
    elsif ( $rel eq 'seniors' )
    {
	$SQL_STRING = "
		SELECT DISTINCT a.taxon_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.orig_no = t2.synonym_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		ORDER BY t.lft";
    }
    
    # For parameter 'classifications', do a straightforward lookup using a
    # second copy of the opinion table.
    
    elsif ( $rel eq 'classifications' )
    {
	$SQL_STRING = "
		SELECT a.taxon_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $opinion_table as o2 using (opinion_no)
			JOIN $tree_table as t on t.orig_no = o2.parent_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr";
    }

    # For parameter 'all_taxa', the first join will differ slightly if select=all
    
    elsif ( $rel eq 'all_taxa' )
    {
	$order_expr = 'ORDER BY t.lft'
	    unless $order_expr;
	
	my $join_expr = $select eq 'all' ? "on a.orig_no = t.orig_no" :
	    "on a.taxon_no = t.${select}_no";
	
	$SQL_STRING = "
		SELECT a.taxon_no
		FROM $auth_table as a JOIN $tree_table as t $join_expr
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr";
    }
    
    # for parameters 'child' and 'all_children', order the results by tree sequence
    
    elsif ( $rel eq 'children' or $rel eq 'all_children' )
    {
	my $level_filter = $rel eq 'child' ? 'and t.depth = t2.depth + 1' : '';
	
	$order_expr = 'ORDER BY t.lft'
	    unless $order_expr;
	
	my $join_expr = $select eq 'all' ? "on a.orig_no = t.orig_no" :
	    "on a.taxon_no = t.${select}_no";
	
	$SQL_STRING = "
		SELECT a.taxon_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft >= t2.lft and t.lft <= t2.rgt
				$level_filter
			JOIN $auth_table as a $join_expr
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr";
    }

    # For parameter 'parents', do a straightforward lookup.
    
    elsif ( $rel eq 'parents' )
    {
	$order_expr = 'ORDER BY t.lft'
	    unless $order_expr;
	
	$SQL_STRING = "
		SELECT a.taxon_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.orig_no = t2.parent_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr";
    }
    
    # for parameter 'all_parents', we need a more complicated procedure in
    # order to do the query efficiently.  This requires using a scratch table
    # and a stored procedure that recursively fills it in.  The scratch table
    # cannot be a temporary table, due to a limitation of MySQL, so we need to
    # use a global table with locks :(
    
    elsif ( $rel eq 'all_parents' )
    {
	my ($result, $result_list);
	
	$result = $dbh->do("LOCK TABLES $ANCESTRY_SCRATCH WRITE,
				    $ANCESTRY_SCRATCH as s WRITE,
				    $auth_table as a READ,
				    $opinion_table as o READ,
				    $tree_table as t READ");
	
	# We need a try block to make sure that the table locks are released
	# no matter what else happens.
	
	try
	{
	    # Clear the scratch table.  We do this at start instead of at end
	    # because it is unavoidably a global table and it is not easy to
	    # guarantee that it will get cleared in all cases if errors occur.
	    
	    $result = $dbh->do("DELETE FROM $ANCESTRY_SCRATCH");
	    
	    # Seed the scratch table with the starting taxon_no values (one or
	    # more).
	    
	    my @tuples;
	    
	    unless ( ref $param_list eq 'ARRAY' )
	    {
		@tuples = "($param_list, 1)";
	    } 
	    else
	    {
		@tuples = map { "($_, 1)" } @$param_list;
	    }
	    
	    $result = $dbh->do("INSERT INTO $ANCESTRY_SCRATCH VALUES " .
			       join(',', @tuples));
	    
	    # Now call a stored procedure which iteratively inserts the
	    # parents of the taxa in $ANCESTRY_SCRATCH back into it until the
	    # top of the taxonomic hierarchy is reached.
	    
	    $result = $dbh->do("CALL compute_ancestry(0)");
	    
	    # Finally, we can use this scratch table to get the information we
	    # need.
	    
	    $SQL_STRING = "
		SELECT t.${select}_no as taxon_no, a.taxon_rank
		FROM $tree_table as t JOIN $ANCESTRY_SCRATCH as s on s.orig_no = t.orig_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		ORDER BY t.lft";
	    
	    $result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
	}
	
	finally {
	    $dbh->do("UNLOCK TABLES");
	    die $_[0] if defined $_[0];
	};
	
	# As a final step, we need to go through the list and see if it
	# includes multiple kingdom-level taxa.  If so, we need to delete
	# everything up to the last kingdom-level taxon found.
	
	my $last_kingdom = 0;
	my @taxon_list;
	
	if ( ref $result_list eq 'ARRAY' )
	{
	    foreach my $i (0..$#$result_list)
	    {
		$last_kingdom = $i if $result_list->[$i]{taxon_rank} eq 'kingdom';
		push @taxon_list, $result_list->[$i]{taxon_no};
	    }
	    
	    splice @taxon_list, 0, $last_kingdom;
	    return @taxon_list;
	}
    }
    
    else
    {
	croak "invalid relationship '$rel'";
    }
    
    # Except for the case of 'all_parents', where the query has to be executed
    # within the scope of a lock statement, execute the query now.
    
    $taxon_nos = $dbh->selectcol_arrayref($SQL_STRING, undef, @$param_list);
    
    # If we got some results, return them.  Otherwise, return the empty list.
    
    if ( ref $taxon_nos eq 'ARRAY' )
    {
	return @$taxon_nos;
    }
    
    else
    {
	return;
    }
}


=head3 getOpinions ( taxa, relationship, options )

Returns a list of Opinion objects having the specified relationship to the
specified base taxon or taxa.  If no matching opinions are found, returns an
empty list.  The parameter C<taxa> may be either a taxon number or a Taxon
object, an array of either of these, or a hash whose keys are taxon numbers.

Possible relationships are:

=over 4

=item child

Returns a list of all opinions by which any of the given taxa are assigned.

=item child_desc

Returns a list of all opinions in which any of the given taxa or their
children are assigned.

=item parent

Returns a list of all opinions in which any of the given taxa are parents
(i.e. being assigned to.)

=item mentioned

Returns a list of all opinions which mention any of the given taxa, whether as
parents or as children.

=item all_opinions

Returns a list of all opinions which match the specified options.  In this
case, the parameter C<taxa> is ignored.  This version of the call should be
used with caution, as it could return a very large number (in the hundreds of
thousands) of records.

=back

Possible options are:

=over 4

=item rank

This option filters the list of resulting opinions, returning only those whose
child taxon has one of the specified ranks (see above).  The value can be
either a single rank, a comma-separated list, or a listref.  In the latter
case, each item can be either a single rank, or a list of [min, max].

=item author

This option filters the list of resulting opinions, returning only those that
match the author name given as the value of the option.  The value of this
option should be a last name only, and selects taxa which match in the 'first
author' or 'second author' fields.  The value can either be a single name, a
comma-separated list, or a listref.

=item pubyr

This option filters the list of resulting opinions, returning only those that
match the publication year given as the value of the option.  This match is done
according to the 'pubyr_rel' option if specified, or an exact match otherwise.

=item pubyr_rel

This option affects the way in which the 'pubyr' option is interpreted, and
otherwise has no effect.  Possible values are '<', '<=', '=', '>=', '>', '<>',
'before', 'after', and 'exact'.

=item reference_no

This option filters the list of resulting opinions, returning only those that are
associated with the reference_no given as the value of the option.  The value
can either be a single reference_no, a comma-separated list, or a listref.

=item person_no

This option filters the list of resulting opinions, returning only those that
have been touched by the person or people given by the value of the option.
The value can either be a single person_no value, a comma-separated list of
them, or a listref.  The filter is done according to the value of the option
'person_rel' if specified, or 'all' if not.

=item person_rel

This option affects the way in which the 'person' option is interpreted, and
otherwise has no effect.  Possible values are 'all', 'authorizer', 'enterer',
'modifier', and 'authorizer_enterer'.  Taxa are selected only if any of the
people specified by the 'person' option have touched them in the role or roles
specified by this option.

=item created

This option filters the list of resulting opinions, returning only those for
which the 'created' date on the record relates to the date specified by the
value of this option according to 'created_rel'.  If the latter option was not
specified, it defaults to 'after'.

=item created_rel

This option affects the way in which the 'created' option is interpreted, and
otherwise has no effect.  Possible values are '<', 'before', '>=', 'after'.

=item status

This option filters the list of resulting opinions according to the taxonomic
status they express.  Accepted values include 'belongs to', 'valid' (a synonym
for 'belongs to'), 'synonym', 'invalid', or any of the actual status values.
The value of this option can be a single value, a comma-separated list, or a
listref.

=back

=cut

sub getOpinions {

    my ($self, $base_taxa, $parameter, $options) = @_;
    
    # Check arguments.  Only throw an error if we were given a reference but
    # couldn't find a taxon number.  If we were passed the undefined value or
    # a taxon number of zero, carp a warning and return undefined.
    
    my $rel = lc $parameter;
    
    my ($base_no, %base_nos);
    
    if ( ref $base_taxa eq 'ARRAY' )
    {
	foreach my $t (@$base_taxa)
	{
	    if ( ref $t )
	    {
		unless ( exists $t->{taxon_no} || exists $t->{orig_no} )
		{
		    carp "could not determine taxon_no";
		    next;
		}
		
		if ( defined $t->{taxon_no} and $t->{taxon_no} > 0 )
		{
		    $base_nos{$t->{taxon_no}} = 1;
		}
		
		elsif ( defined $t->{orig_no} and $t->{orig_no} > 0 )
		{
		    $base_nos{$t->{orig_no}} = 1;
		}
	    }
	    
	    elsif ( $t =~ /^[0-9]+$/ )
	    {
		$base_nos{$t} = 1;
	    }
	}
	
	unless ( keys %base_nos )
	{
	    carp "no valid base taxa were specified";
	    return;
	}
    }
    
    elsif ( ref $base_taxa eq 'HASH' and not exists $base_taxa->{taxon_no} )
    {
	foreach my $t (keys %$base_taxa)
	{
	    if ( $t =~ /^[0-9]+$/ )
	    {
		$base_nos{$t} = 1;
	    }
	}
    }
    
    elsif ( ref $base_taxa )
    {
	croak "could not determine taxon_no from base_taxa" unless
	    exists $base_taxa->{taxon_no} || exists $base_taxa->{orig_no};
	
	$base_no = $base_taxa->{taxon_no} if defined $base_taxa->{taxon_no};
	$base_no = $base_taxa->{orig_no} if defined $base_taxa->{orig_no}
	    and not $base_no > 0;
    }
    
    elsif ( defined $base_taxa && $base_taxa =~ /^[0-9]+$/ )
    {
	$base_no = $base_taxa;
    }
    
    elsif ( $rel eq 'all_opinions' )
    {
	$base_no = 0;	# if the relationship is 'all_opinions', we ignore $base_taxa
    }
    
    else
    {
	carp "no valid base taxa specified";
	return;
    }
    
    # Set option defaults.
    
    my $query_fields = $OPINION_BASIC_FIELDS;
    my $extra_tables = {};
    
    $options ||= {};
    
    my $status;
    
    my $status = defined $options->{status} && $options->{status} ne '' ?
	lc $options->{status} : 'all';
    
    # Set query fields based on the specified options.
    
    if ( defined $options->{fields} and not $options->{id} )
    {
	($query_fields, $extra_tables) = $self->generateOpinionQueryFields($options->{fields});
    }
    
    # Set filter clauses based on the specified options
    
    my (@filter_list, @param_list);
    
    my ($taxon_expr, $join_expr);
    my ($tree_join_expr) = '';
    
    if ( $base_no > 0 )
    {
	$taxon_expr = '= ' . ($base_no + 0);
    }
    
    else
    {
	$taxon_expr = 'in (' . join(',', keys %base_nos) . ')';
    }
    
    if ( $rel eq 'child' )
    {
	push @filter_list, "a2.taxon_no $taxon_expr";
	$join_expr = "o2.orig_no = a2.orig_no";
    }
    
    elsif ( $rel eq 'child_desc' )
    {
	push @filter_list, "a2.taxon_no $taxon_expr";
	$join_expr = "o2.orig_no = t.orig_no";
    }
    
    elsif ( $rel eq 'parent' )
    {
	push @filter_list, "a2.taxon_no $taxon_expr";
	$join_expr = "o2.parent_no = a2.orig_no";
    }
    
    elsif ( $rel eq 'mentioned' )
    {
	push @filter_list, "a2.taxon_no $taxon_expr";
	$join_expr = "o2.orig_no = a2.orig_no or o2.parent_no = a2.orig_no";
    }
    
    elsif ( $rel eq 'all_opinions' )
    {
	$join_expr = '';
    }
    
    else
    {
	croak "invalid relationship '$parameter'";
    }
    
    if ( $status eq 'valid' or $status eq 'belongs to' )
    {
	push @filter_list, "o.status = 'belongs to'";
    }
    elsif ( $status eq 'synonym' ) {
	push @filter_list, "o.status in ('subjective synonym of', 'objective synonym of')";
    }
    elsif ( $status eq 'invalid' ) {
	push @filter_list, "o.status <> 'belongs to'";
    }
    elsif ( $status eq 'all' or $status eq 'any' ) {
	# no filter needed
    }
    else {
	push @filter_list, "o.status = '$status'";
    }
    
    if ( defined $options->{rank} )
    {
	push @filter_list, $self->generateRankFilter('ca', $options->{rank});
	$extra_tables->{ca} = 1;
    }
    
    if ( defined $options->{author} )
    {
	push @filter_list, $self->generateAuthorFilter('o', $options->{author});
	$extra_tables->{ref} = 1;
    }
    
    if ( defined $options->{pubyr} )
    {
	push @filter_list, $self->generatePubyrFilter('o', $options->{pubyr},
						      $options->{pubyr_rel});
	$extra_tables->{ref} = 1;
    }
    
    if ( defined $options->{created} )
    {
	push @filter_list, $self->generateDateFilter('o.created', $options->{created},
						     $options->{created_rel});
    }
    
    if ( defined $options->{modified} )
    {
	push @filter_list, $self->generateDateFilter('o.modified', $options->{modified},
						     $options->{modified_rel});
    }
    
    if ( defined $options->{reference_no} )
    {
	push @filter_list, $self->generateRefnoFilter('o', $options->{reference_no});
    }
    
    if ( defined $options->{person_no} )
    {
	push @filter_list, $self->generatePersonFilter('o', $options->{person_no},
						       $options->{person_rel});
    }
    
    my $filter_expr = join(' and ', @filter_list);
    $filter_expr = "WHERE $filter_expr" if $filter_expr ne '';
    my $extra_joins = $self->generateExtraJoins('o', $extra_tables);
    
    # Now, get ready to do the query.  If we were asked for just the taxon_nos, do
    # that.
    
    if ( $options->{id} )
    {
	return $self->getOpinionIds($rel, $join_expr, $extra_joins, $filter_expr);
    }
    
    # Otherwise, prepare to execute the relevant SQL query
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    my $opinion_cache = $self->{opinion_cache};
    
    my ($result_list);
    
    if ( $rel eq 'all_opinions' )
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $opinion_table as o
		$extra_joins
		$filter_expr
		GROUP BY opinion_no";
    }
    
    elsif ( $rel eq 'child_desc' )
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $opinion_table as o JOIN $opinion_cache as o2 using (opinion_no)
			JOIN $tree_table as t on $join_expr
			JOIN $tree_table as t2 on t.lft >= t2.lft and t.lft <= t2.rgt
			JOIN $auth_table as a2 on t2.orig_no = a2.orig_no
		$extra_joins
		$filter_expr
		GROUP BY opinion_no";
    }
    
    else
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $opinion_table as o JOIN $opinion_cache as o2 using (opinion_no)
			JOIN $auth_table as a2 on $join_expr
		$extra_joins
		$filter_expr
		GROUP BY opinion_no ASC";
    }
    
    $result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
    
    # If we didn't get any results, return nothing.
    
    unless ( ref $result_list eq 'ARRAY' )
    {
	return;
    }
    
    # Otherwise, bless all of the objects that we found (if any) into the
    # proper package.  If the option 'hash' was specified, we construct a hash
    # reference.  Otherwise, we return the list.
    
    my %hashref;
    
    foreach my $t (@$result_list)
    {
	bless $t, "Opinion";
	$hashref{$t->{opinion_no}} = $t if $options->{hash};
    }
    
    return \%hashref if $options->{hash};
    return @$result_list; # otherwise
}


sub getOpinionIds {

    my ($self, $rel, $join_expr, $extra_joins, $filter_expr) = @_;
    
    # Prepare to fetch the requested information.
    
    my $dbh = $self->{dbh};
    my $auth_table = $self->{auth_table};
    my $tree_table = $self->{tree_table};
    my $opinion_table = $self->{opinion_table};
    my $opinion_cache = $self->{opinion_cache};
    
    my ($taxon_nos);
    
    # Now do the appropriate query.
    
    if ( $rel eq 'all_opinions' )
    {
	$SQL_STRING = "
		SELECT o.opinion_no
		FROM $opinion_table as o
			$extra_joins
		$filter_expr
		GROUP BY opinion_no";
    }
    
    elsif ( $rel eq 'child_desc' )
    {
	$SQL_STRING = "
		SELECT o.opinion_no
		FROM $opinion_table as o JOIN $opinion_cache as o2 using (opinion_no)
			JOIN $tree_table as t on $join_expr
			JOIN $tree_table as t2 on t.lft >= t2.lft and t.lft <= t2.rgt
			JOIN $auth_table as a2 on t2.orig_no = a2.orig_no
			$extra_joins
		$filter_expr
		GROUP BY opinion_no";
    }
    
    else
    {
	$SQL_STRING = "
		SELECT o.opinion_no
		FROM $opinion_table as o JOIN $opinion_cache as o2 using (opinion_no)
			JOIN $auth_table as a2 on $join_expr
			$extra_joins
		$filter_expr
		GROUP BY opinion_no";
    }

    $taxon_nos = $dbh->selectcol_arrayref($SQL_STRING);
    
    # If we got some results, return them.  Otherwise, return the empty list.
    
    if ( ref $taxon_nos eq 'ARRAY' )
    {
	return @$taxon_nos;
    }
    
    else
    {
	return;
    }
}


=head3 getTaxonIdTable ( base_taxa, relationship, options )

Returns the name of a newly created temporary table containing a list of
taxon_no values identifying taxa that have the specified relationship to the
specified base taxon or taxa.

Once you get this table, you can join it with the other tables in the database
to carry out any desired query.

   # get a table containing the taxon_no of every taxon in class Mammalia,
   # including junior synonyms and invalid spellings

   $temp_table = $taxonomy->getTaxonIdTable(36651, 'all_children',
			{status => 'synonyms', select => 'all'});
   
   # use this table to select all collections that contain mammalian fossils
   
   my $sth = $dbh->prepare("SELECT distinct collection_no FROM occurrences
			    JOIN $temp_table using (taxon_no) WHERE ...");

The base taxon can be either a taxon_no or a Taxon object, or an array of either.

Valid relationships are:

=over 4

=item all_parents

Return a table containing the taxon_no of every higher taxon that contains the
base taxon, in order from kingdom level on down.

=item all_children

Return a table containing the taxon_no of the base taxon and all of the taxa
that it contains, as a preorder tree traversal.

=item self

Return a table containing just the given taxon numbers.

=item all_taxa

Return a table containing the taxon_no of every taxon in the database.  In
this case, the base taxon is ignored and can be undefined.

=back

Each row in the table represents one matching taxon.  The first column will be
'taxon_no', and other columns can be specified using the 'fields' option.  The
table can then be joined with other tables in order to fetch whatever
information is desired about the resulting taxa.

Valid options include:

=over 4

=item rank

This option filters the list of resulting taxa, returning only those that
match the given rank.  The value can be either a single rank, a
comma-separated list, or a listref.  In the latter case, each item can be
either a single rank, or a list of [min, max].

=item status

This option filters the list of resulting taxa according to taxonomic status.
If 'valid', only valid taxa are returned.  If 'invalid', only invalid taxa are
returned.  If 'synonyms', only valid taxa and junior synonyms.  If 'all', all
matching taxa are returned regardless of taxonomic or nomenclatural status.
The default is 'valid'.

=item exclude_self

If specified, then the base taxon or taxa are excluded from the returned
table.  Otherwise (the default) they are included.  This option is only
relevant for relationships 'all_parents' and 'all_children'.

=item select

This option determines how taxonomic concepts are treated.  The possible
values are as follows:

=over 4

=item spelling

This is the default, and selects the currently accepted spelling of each of
the matching taxonomic concepts.

=item orig

Selects the original spelling of each of the matching taxonomic concepts.

=item all

Selects all spellings of each of the matching taxonomic concepts.

=back

=item fields

The value of this option must be a comma-separated list (or arrayref) of
strings.  Each string should be the name of a field in the taxon_trees table.
One column will be included in the resulting table for each specified field,
in addition to 'taxon_no' which is always present.

=back

=cut

sub getTaxonIdTable {
    
    my ($self, $base_taxa, $parameter, $options) = @_;
    
    # Check arguments.  Only throw an error if we were given a reference but
    # couldn't find a taxon number.  If we were passed the undefined value or
    # a taxon number of zero, carp a warning and return undefined.
    
    my $rel = lc $parameter;
    
    my ($base_no, %base_nos);
    
    if ( ref $base_taxa eq 'ARRAY' )
    {
	foreach my $t (@$base_taxa)
	{
	    if ( ref $t )
	    {
		unless ( exists $t->{taxon_no} || exists $t->{orig_no} )
		{
		    carp "could not determine taxon_no";
		    next;
		}
		
		if ( defined $t->{taxon_no} and $t->{taxon_no} > 0 )
		{
		    $base_nos{$t->{taxon_no}} = 1;
		}
		
		elsif ( defined $t->{orig_no} and $t->{orig_no} > 0 )
		{
		    $base_nos{$t->{orig_no}} = 1;
		}
	    }
	    
	    elsif ( $t =~ /^[0-9]+$/ )
	    {
		$base_nos{$t} = 1;
	    }
	}
	
	unless ( keys %base_nos )
	{
	    carp "no valid base taxa were specified";
	    return;
	}
    }
    
    elsif ( ref $base_taxa eq 'HASH' and not exists $base_taxa->{taxon_no} )
    {
	foreach my $t (keys %$base_taxa)
	{
	    if ( $t =~ /^[0-9]+$/ )
	    {
		$base_nos{$t} = 1;
	    }
	}
    }
    
    elsif ( ref $base_taxa )
    {
	croak "could not determine taxon_no from base_taxa" unless
	    exists $base_taxa->{taxon_no} || exists $base_taxa->{orig_no};
	
	$base_no = $base_taxa->{taxon_no} if defined $base_taxa->{taxon_no};
	$base_no = $base_taxa->{orig_no} if defined $base_taxa->{orig_no}
	    and not $base_no > 0;
    }
    
    elsif ( defined $base_taxa && $base_taxa =~ /^[0-9]+$/ )
    {
	$base_no = $base_taxa;
	$base_nos{$base_no} = 1;
    }
    
    elsif ( $rel eq 'all_taxa' )
    {
	$base_no = 0;	# if the relationship is 'all_taxa', we ignore $base_taxa
    }
    
    else
    {
	carp "base taxon is undefined or zero";
	return;
    }
    
    # Set option defaults.
    
    $options ||= {};
    
    my $status = defined $options->{status} && $options->{status} ne '' ?
	lc $options->{status} : 'valid';
    my $select = defined $options->{select} ? lc $options->{select} : 'spelling';
    
    unless ( $select eq 'spelling' or $select eq 'orig' 
	     or $select eq 'trad' or $select eq 'all' )
    {
	croak "invalid value '$options->{select}' for option 'select'";
    }
    
    $select = 'trad' if $options->{trad} and $select eq 'spelling';
    
    # Construct the appropriate modifiers based on the options.
    
    my @filter_list;
    
    if ( $rel eq 'all_children' and defined $base_no )
    {
	push @filter_list, "a2.taxon_no = $base_no";
    }
    
    elsif ( $rel eq 'all_children' )
    {
	my $list = '(' . join(',', keys %base_nos) . ')';
	push @filter_list, "a2.taxon_no in $list";
    }
    
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
	push @filter_list, $self->generateRankFilter('a', $options->{rank});
    }
    
    if ( $options->{exclude_self} )
    {
	if ( $rel eq 'all_parents' )
	{
	    push @filter_list, "s.gen > 1";
	}
	
	elsif ( $rel eq 'all_children' )
	{
	    push @filter_list, "t.lft != t2.lft";
	}
    }
    
    my $filter_expr = join(' and ', @filter_list);
    $filter_expr = "WHERE $filter_expr" if $filter_expr ne '';
    
    # Select extra fields for the new table, if the option 'fields' was
    # specified.
    
    my $create_string = '';
    my $select_string = '';
    
    my $field_list = $options->{fields};
    
    if ( defined $field_list )
    {
	unless ( ref $field_list )
	{
	    my @fields = split /\s*,\s*/, $field_list;
	    $field_list = \@fields;
	}
	
	if ( ref $field_list ne 'ARRAY' )
	{
	    croak "option 'fields' must be a scalar or arrayref";
	}
	
	my %seen;
	
	# Go through the specified field names, making sure to ignore
	# duplicates.
	
	foreach my $f ( @$field_list )
	{
	    next if $seen{$f}; $seen{$f} = 1;
	    
	    if ( $TAXON_FIELD{$f} )
	    {
		$create_string .= "$f int unsigned not null,\n";
		$select_string .= ",t.$f";
	    }
	    
	    else
	    {
		carp "unknown value '$f' for option 'fields'";
	    }
	}
    }
    
    # Prepare to fetch the requested information
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    # Create a temporary table to hold the requested information.
    
    my $table_name = $self->createTempTable("taxon_no int unsigned not null,
					     $create_string
					     UNIQUE KEY (taxon_no)");
    
    my $result;
    
    # We need a try block to make sure that we drop the temporary table and
    # any locks we might hold if an error occurs.

    try
    {
	# The relationships 'all_taxa' and 'all_children' are fairly
	# straightforward. 
	
	if ( $rel eq 'all_taxa' and $select eq 'all' )
	{
	    $result = $dbh->do("INSERT INTO $table_name
		SELECT a.taxon_no $select_string
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o using (opinion_no)
		$filter_expr");
	}
	
	elsif ( $rel eq 'all_taxa' )
	{
	    $result = $dbh->do("INSERT INTO $table_name
		SELECT t.${select}_no $select_string
		FROM $tree_table as t
			LEFT JOIN $opinion_table as o using (opinion_no)
		$filter_expr");
	}
	
	elsif ( $rel eq 'all_children' and $select eq 'all' )
	{
	    $result = $dbh->do("INSERT INTO $table_name
		SELECT a.taxon_no $select_string
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft >= t2.lft and t.lft <= t2.rgt
			JOIN $auth_table as a on t.orig_no = a.orig_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
	        $filter_expr");
	}
	
	elsif ( $rel eq 'all_children' )
	{
	    $result = $dbh->do("INSERT INTO $table_name
		SELECT t.${select}_no $select_string
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft >= t2.lft and t.lft <= t2.rgt
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
	        $filter_expr");
	}
	
	# for parameter 'all_parents', we need a more complicated procedure in
	# order to do the query efficiently.  This requires using a scratch
	# table and a stored procedure that recursively fills it in.  The
	# scratch table cannot be a temporary table, due to a limitation of
	# MySQL, so we need to use a global table with locks :(
	
	elsif ( $rel eq 'all_parents' )
	{
	    $result = $dbh->do("LOCK TABLES $ANCESTRY_SCRATCH WRITE,
				    $ANCESTRY_SCRATCH as s WRITE,
				    $auth_table as a READ,
				    $opinion_table as o READ,
				    $tree_table as t READ");
	
	    # Clear the scratch table.  We do this at start instead of at end
	    # because it is unavoidably a global table and it is not easy to
	    # guarantee that it will get cleared in all cases if errors occur.
	    
	    $result = $dbh->do("DELETE FROM $ANCESTRY_SCRATCH");
	    
	    # Seed the scratch table with the starting taxon_no values (one or
	    # more).
	    
	    my @tuples;
	    
	    unless ( ref $base_no eq 'ARRAY' )
	    {
		@tuples = "($base_no, 1)";
	    } 
	    else
	    {
		@tuples = map { "($_, 1)" } @$base_no;
	    }
	    
	    $result = $dbh->do("INSERT IGNORE INTO $ANCESTRY_SCRATCH VALUES " .
			       join(',', @tuples));
	    
	    # Now call a stored procedure which iteratively inserts the
	    # parents of the taxa in $ANCESTRY_SCRATCH back into it until the
	    # top of the taxonomic hierarchy is reached.
	    
	    $result = $dbh->do("CALL compute_ancestry(0)");
	    
	    # Finally, we can copy the id numbers from the scratch table into
	    # our temporary table.  This step makes the routine less
	    # efficient, but is unavoidable since we could not make the
	    # scratch table a temporary one.
	    
	    if ( $select eq 'all' )
	    {
		$result = $dbh->do("INSERT INTO $table_name
		SELECT a.taxon_no $select_string
		FROM $auth_table as a JOIN $ANCESTRY_SCRATCH as s using (orig_no)
			JOIN $tree_table as t on a.orig_no = t.orig_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
		$filter_expr");
	    }
	    
	    else
	    {
		$result = $dbh->do("INSERT INTO $table_name
		SELECT t.${select}_no $select_string
		FROM $tree_table as t JOIN $ANCESTRY_SCRATCH as s on s.orig_no = t.orig_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
		$filter_expr");
	    }
	}
	
	# For relationship 'self', we just need to load the given taxon
	# numbers into the table.
	
	elsif ( $rel eq 'self' )
	{
	    my $value_string = '(' . join('),(', keys %base_nos) . ')';
	    
	    $result = $dbh->do("INSERT INTO $table_name VALUES $value_string");
	}
	
	else
	{
	    croak "invalid relationship '$parameter'";
	}
    }
    
    # Whether or not an error occurred, we drop our table locks (if any).  If
    # an error did occur, we drop the temporary table before re-throwing it.
    
    finally {
	$dbh->do("UNLOCK TABLES");
	if ( defined $_[0] )
	{
	    $dbh->do("DROP TABLE IF EXISTS $table_name");
	    die $_[0];
	}
    };
    
    return $table_name;
}


=head2 Other functions

=head3 isValidName ( taxon_name, options )

Returns true if the specified name is a valid taxon name, false otherwise.
This function can be called either directly or as a method.

    if ( $taxonomy->validName($name, { strict => 1 }) ) { ... }
    if ( Taxonomy::validName($name, { strict => 1 }) ) { ... }

The following options are available:

=over 4

=item strict

If this option is specified, the name must contain nothing but Roman letters
and spaces (with parentheses surrounding a subgenus qualification) and must
strictly follow the letter case rules.  All names above species-level must
begin with a single upper-case letter, while all species and subspecies names
must be in lower case.  Name components must be separated by a single space.

Otherwise, the "loose" rule is applied: spacing and case are ignored and
periods and the % wildcard are allowed in the name.

=back

=cut

sub isValidName {

    # Start by parsing the argument list.  If the first argument is an object
    # of class Taxonomy, ignore it.  This sub can be called either as a method
    # or directly as a function.
    
    shift if ref $_[0] eq 'Taxonomy';
    my ($name, $options) = @_;
    $options = {} unless ref $options;
    
    # If the name matches the proper pattern, return true.
    
    if ( $options->{strict} )
    {
	return 1
	if $name =~ /^[A-Z][a-z]+(?:\s+\([A-Z][a-z]+\))?(?:\s+[a-z]+)?(?:\s+[a-z.]+)?\s*$/;
    }
    
    else
    {
	return 1
	if $name =~ /^\s*[a-z%.]+(?:\s*\([a-z%.]+\))?(?:\s+[a-z%.]+)?(?:\s+[a-z%.]+)?\s*$/i;
    }
    
    # Otherwise, return false.
    
    return undef;
}


=head3 splitTaxonName ( taxon_name )

Splits the given name into the following components, some of which may be
empty: genus, subgenus, species, subspecies.  Returns these four strings.
This routine can be called either as a method or as a direct function.

    ($g, $sg, $sp, $ssp) = $taxonomy->splitName($name);
    ($g, $sg, $sp, $ssp) = Taxonomy::splitName($name);

If the given name is not a valid taxonomic name according to the "loose" rules
(see C<validName> above) then a list of four empty strings is returned.

You should not use this function to check the validity of a taxon name.  Instead,
use C<validName>.

=cut

sub splitTaxonName {

    # Start by parsing the argument list.  If the first argument is an object
    # of class Taxonomy, ignore it.  This sub can be called either as a method
    # or directly as a function.
    
    shift if ref $_[0] eq 'Taxonomy';
    my ($name) = @_;
    
    my ($genus,$subgenus,$species,$subspecies) = ("","","","");
    
    if ( $name =~ /^\s*([a-z%.]+)(?:\s\(([a-z%.]+)\))?(?:\s([a-z%.]+))?(?:\s([a-z%.]+))?\s*/i )
    {
	$genus = $1 if ($1);
	$subgenus = $2 if ($2);
	$species = $3 if ($3);
	$subspecies = $4 if ($4);
    }
    
    return ($genus, $subgenus, $species, $subspecies);
}


=head3 guessTaxonRank ( name )

Return the most likely rank for the given name, based upon its composition.
This routine can be called either as a method or as a direct function.  If no
rank can be guessed, returns undefined.

=cut

sub guessTaxonRank {

    # Start by parsing the argument list.  If the first argument is an object
    # of class Taxonomy, ignore it.  This sub can be called either as a method
    # or directly as a function.
    
    shift if ref $_[0] eq 'Taxonomy';
    my ($name) = @_;
    
    if ( $name =~ /^[a-z.]+(\s*\([a-z]+\))?(\s+[a-z.]+)?(\s+[a-z.]+)?\s*/i )
    {
        return "subspecies" if $3;
	return "species" if $2;
	return "subgenus" if $1;
    }
    
    if ($name =~ /ini$/)	{
        return "tribe";
    } elsif ($name =~ /inae$/)	{
        return "subfamily";
    } elsif ($name =~ /idae$/)	{
        return "family";
    } elsif ($name =~ /eae$/)	{
        return "family";
    } elsif ($name =~ /oidea$/)	{
        return "superfamily";
    } elsif ($name =~ /ida$/)	{
        return "order";
    } elsif ($name =~ /formes$/)	{
        return "order";
    } elsif ($name =~ /ales$/)	{
        return "order";
    } else {
	return;
    }
}


=head3 isUsedTaxon ( taxon_no )

Return true if the taxon has at least one genus- or species- level child.

=cut

sub isUsedTaxon {
    
    my ($self, $taxon_no) = @_;
    
    # Make sure that we have a positive taxon_no value, otherwise return false.
    
    return unless $taxon_no > 0;
    $taxon_no = $taxon_no + 0;
    
    # Do the necessary query.  We only need to find one genus or species-level
    # entry to ensure that this name is still used.
    
    my $dbh = $self->{dbh};
    my $auth_table = $self->{auth_table};
    my $tree_table = $self->{tree_table};
    
    my $result_list = $dbh->selectcol_arrayref("
	SELECT a.orig_no
	FROM $auth_table as a JOIN $tree_table as t using (orig_no)
		JOIN $tree_table as t2 on t.lft > t2.lft and t.lft <= t2.rgt
		JOIN $auth_table as a2 on t2.orig_no = a2.orig_no
	WHERE a2.taxon_no = $taxon_no AND a.taxon_rank in ('genus', 'subgenus', 'species')
	LIMIT 1");
    
    # If we found something, return 1 (i.e. the taxon is used)
    
    if ( $result_list && @$result_list > 0 )
    {
	return 1;
    }
    
    # Otherwise, return false (i.e. the taxon is disused)
    
    else
    {
	return;
    }
}


# The following routines are used by the methods defined above to establish
# the proper query fields and parameters.
#
# =========================================================================

# generateQueryFields ( include_list )
# 
# The parameter 'include_list' can be either an array of strings or a
# comma-separated concatenation of strings.  Possible values include: 'attr',
# 'oldattr', 'ref', 'link', 'lft', 'kingdom', 'type'.
# 
# In any case, this routine returns a field list and a hash which lists
# extra tables to be joined in the query.

sub generateQueryFields {

    my ($self, $fields_list) = @_;
    
    # Return the default if our parameter is undefined.
    
    unless ( defined $fields_list )
    {
	return $AUTH_BASIC_FIELDS, {};
    }
    
    # Next, turn $list into a reference to an actual list unless it already
    # is one.
    
    unless ( ref $fields_list )
    {
	my @strings = split(/\s*,\s*/, $fields_list);
	$fields_list = \@strings;
    }
    
    elsif ( ref $fields_list ne 'ARRAY' )
    {
	croak "option 'fields' must be either a string or an arrayref";
    }
    
    # Now go through the list of strings and add the appropriate fields and
    # tables for each.
    
    my $fields = $AUTH_BASIC_FIELDS;
    my %tables;
    
    foreach my $inc (@$fields_list)
    {
	if ( $inc eq 'attr' )
	{
	    $fields .= $ATTR_FIELDS;
	    $tables{ref} = 1;
	}
	
	elsif ( $inc eq 'oldattr' )
	{
	    $fields .= $OLDATTR_FIELDS;
	    $tables{ref} = 1;
	}
	
	elsif ( $inc eq 'ref' )
	{
	    $fields .= $REF_FIELDS;
	    $tables{ref} = 1;
	}
	
	elsif ( $inc eq 'link' )
	{
	    $fields .= $LINK_FIELDS;
	}
	
	elsif ( $inc eq 'orig' )
	{
	    $fields .= $ORIG_FIELDS;
	    $tables{ora} = 1;
	}
	
	elsif ( $inc eq 'lft' )
	{
	    $fields .= $LFT_FIELDS;
	}
	
	elsif ( $inc eq 'parent' )
	{
	    $fields .= $PARENT_FIELDS;
	    $tables{pa} = 1;
	}
	
	elsif ( $inc eq 'kingdom' )
	{
	    $fields .= $KINGDOM_FIELDS;
	}
	
	elsif ( $inc eq 'tt' )
	{
	    $fields .= $TT_FIELDS;
	    $tables{tta} = 1;
	}
	
	elsif ( $inc eq 'person' )
	{
	    $fields .= $PERSON_FIELDS;
	    $tables{pp} = 1;
	}
	
	elsif ( $inc eq 'specimen' )
	{
	    $fields .= $SPECIMEN_FIELDS;
	}
	
	elsif ( $inc eq 'pages' )
	{
	    $fields .= $PAGES_FIELDS;
	}
	
	elsif ( $inc eq 'created' )
	{
	    $fields .= $CREATED_FIELDS;
	}
	
	elsif ( $inc eq 'modshort' )
	{
	    $fields .= $MODSHORT_FIELDS;
	}
	
	elsif ( $inc eq 'comments' )
	{
	    $fields .= $COMMENT_FIELDS;
	}
	
	else
	{
	    carp "unrecognized value '$inc' in option 'fields'";
	}
    }
    
    return ($fields, \%tables);
}


# generateOpinionQueryFields ( include_list )
# 
# The parameter 'include_list' can be either an array of strings or a
# comma-separated concatenation of strings.  Possible values include: 'attr',
# 'oldattr', 'ref', 'link', 'lft', 'kingdom', 'type'.
# 
# In any case, this routine returns a field list and a hash which lists
# extra tables to be joined in the query.

sub generateOpinionQueryFields {

    my ($self, $fields_list) = @_;
    
    # Return the default if our parameter is undefined.
    
    unless ( defined $fields_list )
    {
	return $OPINION_BASIC_FIELDS, '';
    }
    
    # Next, turn $list into a reference to an actual list unless it already
    # is one.
    
    unless ( ref $fields_list )
    {
	my @strings = split(/\s*,\s*/, $fields_list);
	$fields_list = \@strings;
    }
    
    elsif ( ref $fields_list ne 'ARRAY' )
    {
	croak "option 'fields' must be either a string or an arrayref";
    }
    
    # Now go through the list of strings and add the appropriate fields and
    # tables for each.
    
    my $fields = $OPINION_BASIC_FIELDS;
    my %tables;
    
    foreach my $inc (@$fields_list)
    {
	if ( $inc eq 'attr' )
	{
	    $fields .= $OPINION_ATTR_FIELDS;
	    $tables{ref} = 1;
	}
	
	elsif ( $inc eq 'oldattr' )
	{
	    $fields .= $OPINION_OLDATTR_FIELDS;
	    $tables{ref} = 1;
	}
	
	elsif ( $inc eq 'ref' )
	{
	    $fields .= $REF_FIELDS;
	    $tables{ref} = 1;
	}
	
	elsif ( $inc eq 'parent' )
	{
	    $fields .= $PARENT_FIELDS;
	    $tables{pa} = 1;
	}
	
	elsif ( $inc eq 'child' )
	{
	    $fields .= $CHILD_FIELDS;
	    $tables{ca} = 1;
	}
	
	elsif ( $inc eq 'person' )
	{
	    $fields .= $PERSON_FIELDS;
	    $tables{pp} = 1;
	}
	
	elsif ( $inc eq 'pages' )
	{
	    $fields .= $PAGES_FIELDS;
	}
	
	elsif ( $inc eq 'created' )
	{
	    $fields .= $OPINION_CREATED_FIELDS;
	}
	
	elsif ( $inc eq 'modshort' )
	{
	    $fields .= $OPINION_MODSHORT_FIELDS;
	}
	
	elsif ( $inc eq 'comments' )
	{
	    $fields .= $COMMENT_FIELDS;
	}
	
	else
	{
	    carp "unrecognized value '$inc' in option 'fields'";
	}
    }
    
    # Change all a.* to o.*
    
    $fields =~ s/ a\./ o\./g;
    
    # Return results
    
    return ($fields, \%tables);
}


# generateExtraJoins( main_table, table_hash )
# 
# Converts the table hash into a string listing extra joins to be included
# in a query.

sub generateExtraJoins {

    my ($self, $main_table, $tables, $select) = @_;
    
    my $auth_table = $self->{auth_table};
    my $tree_table = $self->{tree_table};
    
    my $extra_joins = '';
    
    # Return an empty string unless we actually have some joins to make
    
    return $extra_joins unless ref $tables eq 'HASH' and %$tables;
    
    # Create the necessary join expressions.
    
    $extra_joins .= "JOIN refs as r on $main_table.reference_no = r.reference_no\n" 
	if $tables->{ref};
    $extra_joins .= "LEFT JOIN $auth_table as tta on tta.taxon_no = $main_table.type_taxon_no\n"
	if $tables->{tta};
    $extra_joins .= "LEFT JOIN $auth_table as ora on ora.taxon_no = $main_table.orig_no\n"
	if $tables->{ora};
    $extra_joins .= "LEFT JOIN $auth_table as ca on ca.taxon_no = $main_table.child_spelling_no\n"
	if $tables->{ca};
    
    if ( $tables->{pa} and $main_table !~ /^o/ )
    {
	$extra_joins .= "LEFT JOIN $tree_table as pt on pt.orig_no = t.parent_no\n";
	$extra_joins .= "LEFT JOIN $auth_table as pa on pa.taxon_no = pt.${select}_no\n";
    }
    
    elsif ( $tables->{pa} )
    {
	$extra_joins .= "LEFT JOIN $auth_table as pa on pa.taxon_no = $main_table.parent_spelling_no\n";
    }
    
    $extra_joins .= "
		LEFT JOIN person as pp1 on pp1.person_no = $main_table.authorizer_no
		LEFT JOIN person as pp2 on pp2.person_no = $main_table.enterer_no
		LEFT JOIN person as pp3 on pp3.person_no = $main_table.modifier_no\n"
	if $tables->{pp};
    
    # Return results
    
    return $extra_joins;
}


# generateExtraLocks( table_hash )
# 
# Converts the table hash into a string specifying extra locks that must be
# acquired in order to do the query.

sub generateExtraLocks {

    my ($self, $tables) = @_;
    
    my $auth_table = $self->{auth_table};
    my $tree_table = $self->{tree_table};
    
    # Return an empty string unless we actually have some joins to make
    
    return '' unless ref $tables eq 'HASH' and %$tables;
    
    # Create the necessary lock expressions.
    
    my $extra_locks = '';
    
    $extra_locks .= ", refs as r READ" if $tables->{ref};
    $extra_locks .= ", $auth_table as tta READ" if $tables->{tta};
    $extra_locks .= ", $auth_table as ora READ" if $tables->{ora};
    $extra_locks .= ", $auth_table as ca READ" if $tables->{ca};
    $extra_locks .= ", $auth_table as pa READ" if $tables->{pa};
    $extra_locks .= ", $tree_table as pt READ" if $tables->{pa};
    $extra_locks .= ", person as pp1 READ, person as pp2 READ, person as pp3 READ"
	if $tables->{pp};
    
    # Return results
    
    return $extra_locks;
}


# generateExcludeFilter ( table, exclude_list, include_list )
# 
# The parameter 'exclude_list' can be either an array or comma-separated list
# of taxon numbers or a list of Taxon objects.  We return a list of filter
# clauses which will exclude the specified taxa and their children from a
# query.
# 
# If specified, 'include_list' must be a list of taxon_no values.  These are
# compared to 'exclude_list' in order to ignore irrelevant values in the
# latter (i.e. taxa which are not actually children of any of the taxa
# included in the query) in order to improve query performance.

sub generateExcludeFilter {

    my ($self, $table, $exclude_list, $include_list) = @_;
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    
    # First, extract a list of taxon numbers from $exclude_list.
    
    my (@exclude_nos, @include_nos);
    
    unless ( ref $exclude_list )
    {
	my @input_list = split(/\s*,\s*/, $exclude_list);
	$exclude_list = \@input_list;
    }
    
    if ( ref $exclude_list eq 'ARRAY' )
    {
	foreach my $t (@$exclude_list)
	{
	    if ( ref $t eq 'ARRAY' )
	    {
		croak "invalid value for option 'exclude': array must contain numbers or Taxon objects";
	    }
	    
	    elsif ( ref $t )
	    {
		if ( defined $t->{taxon_no} and $t->{taxon_no} =~ /^[0-9]+$/ )
		{
		    push @exclude_nos, $t->{taxon_no};
		}
		
		elsif ( defined $t->{orig_no} and $t->{orig_no} =~ /^[0-9]+$/ )
		{
		    push @exclude_nos, ($t->{orig_no} + 0);
		}
	    }
	    
	    elsif ( $t =~ /^[0-9]+$/ )
	    {
		push @exclude_nos, $t;
	    }
	}
    }
    
    else
    {
	croak "invalid value for option 'exclude': must be an arrayref or comma-separated list";
    }
    
    # Return nothing if we don't have any valid taxon_nos to exclude
    
    return unless @exclude_nos;
    
    # Otherwise, we continue by querying for a lft/rgt pair which encompasses
    # all included taxa.  This will allow us to make sure that only relevant
    # exclusions are included in the returned list.
    
    my ($include_lft, $include_rgt);
    
    if ( ref $include_list eq 'ARRAY' )
    {
	my $include_string = join(',', @$include_list);
	
	$SQL_STRING = "
		SELECT min(t.lft), max(t.rgt)
		FROM $tree_table as t JOIN $auth_table as a using (orig_no)
		WHERE a.taxon_no in ($include_string)";
	
	($include_lft, $include_rgt) = $dbh->selectrow_array($SQL_STRING);
    }
    
    # Next, get lft/rgt values for each of the excluded taxa and generate
    # filters to exclude all of the relevant ones.
    
    my $taxon_list = join(',', @exclude_nos);
    
    $SQL_STRING = "
		SELECT t.lft, t.rgt
		FROM $tree_table as t JOIN $auth_table as a using (orig_no)
		WHERE a.taxon_no in ($taxon_list)";
    
    my $result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
    
    return unless ref $result_list eq 'ARRAY';
    
    my @filter_clauses;
    
    # Only generate a filter clause if the range is within the interval specified
    # by $include_lft .. $include_rgt, because otherwise it would be irrelevant.
    
    foreach my $row (@$result_list)
    {
	next if defined $include_lft and 
	    ($row->{lft} < $include_lft or $row->{lft} > $include_rgt);
	
	next unless $row->{lft} > 0 and $row->{rgt} > 0;
	
	push @filter_clauses, "t.lft not between $row->{lft} and $row->{rgt}";
    }
    
    return @filter_clauses;
}


# generateRankFilter ( table, rank_list )
# 
# The parameter 'rank_list' can be either an array of strings or a
# comma-separated concatenation of strings.  We return a list of filter
# clauses which will select the specified rank or ranks.

sub generateRankFilter {
    
    my ($self, $table, $rank_list) = @_;
    
    # If the parameter is undefined, we return nothing (so the query goes
    # ahead with no rank filtering).
    
    return unless defined $rank_list;
    
    # Next, we turn $rank_list into a reference to an actual list unless it
    # already is one.
    
    unless ( ref $rank_list )
    {
	my @strings = split /\s*,\s*/, $rank_list;
	$rank_list = \@strings;
    }
    
    elsif ( ref $rank_list ne 'ARRAY' )
    {
	croak "option 'rank' must be either a string or an arrayref";
    }
    
    # Now go through the list of ranks and collect up a list of distinct values.
    # Ignore invalid ranks, but carp a warning about them.
    
    my %include_ranks;
    
    foreach my $rank (@$rank_list)
    {
	if ( $rank =~ /^[0-9]+$/ )
	{
	    $include_ranks{$rank} = 1;
	}
	
	elsif ( $TAXONOMIC_RANK{$rank} )
	{
	    $include_ranks{$TAXONOMIC_RANK{$rank}} = 1;
	}
	
	elsif ( $rank eq 'genus or below' )
	{
	    $include_ranks{2} = 1;
	    $include_ranks{3} = 1;
	    $include_ranks{4} = 1;
	    $include_ranks{5} = 1;
	}
	
	elsif ( $rank eq 'above genus' )
	{
	    $include_ranks{above_genus} = 1;
	}
	
	else
	{
	    carp "invalid rank '$rank'";
	}
    }
    
    # If we have at least one valid rank, generate a clause to select only
    # taxa which have those ranks.
    
    if ( $include_ranks{above_genus} )
    {
	return "$table.taxon_rank > 5";
    }
    
    elsif ( %include_ranks )
    {
	return "$table.taxon_rank in (" . join(",", keys %include_ranks) . ")";
    }
    
    # Otherwise, if all of the ranks were invalid, generate a clause which
    # will select no taxa (this is probably better than no filter at all in
    # most cases).
    
    else
    {
	return "$table.taxon_rank = 0";
    }
}


# generateAuthorFilter ( author_list )
# 
# The parameter 'author_list' can be either an array of strings or a
# comma-separated concatenation of strings.  We return a filter clause which
# will select only taxa whose listed first or second author last name matches
# one of the values in the list.

sub generateAuthorFilter {

    my ($self, $table, $author_list) = @_;
    
    # If the parameter is undefined, we return nothing (so the query goes
    # ahead with no author filtering).
    
    return unless defined $author_list;
    
    # Next, we turn $author_list into a reference to an actual list unless it
    # already is one.
    
    unless ( ref $author_list )
    {
	my @strings = split /\s*,\s*/, $author_list;
	$author_list = \@strings;
    }
    
    elsif ( ref $author_list ne 'ARRAY' )
    {
	croak "option 'author' must be either a string or an arrayref";
    }
    
    # Now go through the list of authors and collect up a list of distinct
    # values to construct a selection expression.
    
    my %include_authors;
    
    foreach my $author (@$author_list)
    {
	$author =~ s/\s+$//;
	
	if ( $author ne '' )
	{
	    my $dbh = $self->{dbh};
	    my $clean_string = $dbh->quote($author);
	    $include_authors{$clean_string} = 1;
	}
    }
    
    # Create the necessary expression.
    
    if ( keys %include_authors > 1 )
    {
	my $expr = "(" . join(",", keys %include_authors) . ")";
	
	return "if($table.refauth, r.author1last in $expr or r.author2last in $expr,
			$table.author1last in $expr or $table.author2last in $expr)";
    }
    
    elsif ( keys %include_authors == 1 )
    {
	my ($auth) = keys %include_authors;
	
	return "if($table.refauth, r.author1last=$auth or r.author2last = $auth,
			$table.author1last=$auth or $table.author2last=$auth)";
    }
    
    # If there are no valid authors, we return an expression which will select
    # no taxa (better than no filter at all).
    
    else
    {
	return "$table.author1last = 'SELECT_NOTHING'";
    }
}


# generatePubyrFilter ( value, relation )
# 
# The parameter 'relation' can be any of '<', '<=', '=', '>=', '>', '<>',
# 'before', 'after', 'exact'.  We return a filter clause which will select
# only taxa whose publication year has the specified relation to the specified
# value.

our (%NUM_REL_MAP) = ( 'before' => '<', 'after' => '>=', 'exact' => '=' );

sub generatePubyrFilter {

    my ($self, $table, $value, $relation) = @_;
    
    # First validate the arguments.  If no value is given, we return nothing.
    # If an invalid value is given, we return an expression which will select no
    # taxa (better than no filter at all).
    
    unless ( defined $value )
    {
	return;
    }
    
    unless ( lc $relation =~ /^(?:<|<=|=|>=|>|<>|!=|before|after|exact|)$/ )
    {
	croak("invalid value for option 'pubyr_rel'");
    }
    
    unless ( $value =~ /^\s*[0-9]+\s*$/ )
    {
	carp("invalid value for option 'pubyr': must be a number");
	return "$table.pubyr = -1";
    }
    
    # Now construct the required expression.  The relation defaults to '=' if
    # not explicitly specified.
    
    my $rel = $NUM_REL_MAP{lc $relation} || $relation || '=';
    
    return "if($table.refauth, r.pubyr $rel $value and r.pubyr <> '',
			$table.pubyr $rel $value and $table.pubyr <> '')";
}


# generateDateFilter ( field, value, relation )
# 
# The parameter 'relation' can be any of '<', '>=', 'before', 'after'.  We
# return a filter clause which will select only taxa whose record date of
# creation/modification has the specified relation to the specified value.
# The field to be compared is specified by the parameter 'field'.

sub generateDateFilter {

    my ($self, $field, $value, $relation) = @_;
    
    # First validate the arguments.  If no value is given, we return nothing.
    # If an invalid value is given, we return an expression which will select no
    # taxa (better than no filter at all).
    
    unless ( defined $value )
    {
	return;
    }
    
    unless ( lc $relation =~ /^(?:<|>=|before|after|)$/ )
    {
	croak("invalid value for option 'created_rel'");
    }
    
    unless ( $value =~ /^\d+-\d+-\d+[\d: ]*$/ )
    {
	return "$field = 'SELECT_NOTHING'";
    }
    
    # Now construct the required expression.
    
    my $rel = $NUM_REL_MAP{lc $relation} || $relation || '>=';
    $rel = '<' if $rel eq '<=';		# 'before' should map to <, not <=.
    
    return "$field $rel '$value'";
}


# generateRefnoFilter ( ref_no_list )
# 
# The parameter 'ref_no_list' can be either an array of numbers or a
# comma-separated concatenation of numbers.  We return a filter clause which
# will select only taxa whose reference number matches one of the values in
# the list.

sub generateRefnoFilter {

    my ($self, $table, $ref_no_list) = @_;
    
    # If the parameter is undefined, we return nothing (so the query goes
    # ahead with no ref_no filtering).
    
    return unless defined $ref_no_list;
    
    # Next, we turn $ref_no_list into a reference to an actual list unless it
    # already is one.
    
    unless ( ref $ref_no_list )
    {
	my @strings = split /\s*,\s*/, $ref_no_list;
	$ref_no_list = \@strings;
    }
    
    elsif ( ref $ref_no_list ne 'ARRAY' )
    {
	croak "option 'reference_no' must be either a string or an arrayref";
    }
    
    # Now go through the list of values and construct.
    
    my %include_refno;
    
    foreach my $reference_no (@$ref_no_list)
    {
	if ( $reference_no > 0 )
	{
	    $include_refno{int($reference_no)} = 1;
	}
    }
    
    if ( keys %include_refno > 1 )
    {
	my $expr = "('" . join("','", keys %include_refno) . "')";
	
	return "$table.reference_no in $expr";
    }
    
    elsif ( keys %include_refno == 1 )
    {
	my ($reference_no) = keys %include_refno;
	
	return "$table.reference_no = $reference_no";
    }
    
    # If there are no valid reference_no values, we return an expression which will
    # select no taxa (better than no filter at all).
    
    else
    {
	return "$table.reference_no = -1";
    }
}


# generatePersonFilter ( person_list, role )
# 
# The parameter 'role' can be any of 'all', 'authorizer', 'enterer',
# 'modifier', or 'authorizer_enterer'.  We return a filter clause which will
# select only those taxa which were touched in the specified role by one of
# the people (person_no values) in the specified list.

sub generatePersonFilter {

    my ($self, $table, $person_list, $role) = @_;
    
    # First validate the arguments.  If no role or value is given, we return
    # nothing.
    
    unless ( defined $role and defined $person_list )
    {
	return;
    }
    
    unless ( lc $role =~ /^!?(?:all|any|authorizer|enterer|authorizer_enterer|modifier)$/ )
    {
	croak("invalid value for option 'person_rel'");
    }
    
    # Go through the list of authors and collect up a list of distinct values
    # to construct a selection expression.
    
    my %include_persons;
    
    foreach my $person_no (@$person_list)
    {
	if ( $person_no > 0 )
	{
	    $include_persons{int($person_no)} = 1;
	}
    }
    
    # Create the necessary expression.
    
    if ( keys %include_persons )
    {
	my $expr = "(" . join(",", keys %include_persons) . ")";
	my $rel = lc $role;
	
	if ( $rel eq 'authorizer' )
	{
	    return "$table.authorizer_no in $expr";
	}
	elsif ( $rel eq '!authorizer' )
	{
	    return "$table.authorizer_no not in $expr";
	}
	elsif ( $rel eq 'enterer' )
	{
	    return "$table.enterer_no in $expr";
	}
	elsif ( $rel eq '!enterer' )
	{
	    return "$table.enterer_no not in $expr";
	}
	elsif ( $rel eq 'modifier' )
	{
	    return "$table.modifier_no in $expr";
	}
	elsif ( $rel eq '!modifier' )
	{
	    return "$table.modifier_no not in $expr";
	}
	elsif ( $rel eq 'authorizer_enterer' )
	{
	    return "($table.authorizer_no in $expr or $table.enterer_no in $expr)";
	}
	elsif ( $rel eq '!authorizer_enterer' )
	{
	    return "$table.authorizer_no not in $expr and $table.enterer_no not in $expr";
	}
	elsif ( $rel eq 'all' or $rel eq 'any' )
	{
	    return "($table.authorizer_no in $expr or $table.enterer_no in $expr or $table.modifier_no in $expr)";
	}
	elsif ( $rel eq '!all' or $rel eq '!any' )
	{
	    return "$table.authorizer_no not in $expr and $table.enterer_no not in $expr and $table.modifier_no not in $expr";
	}
    }
    
    # If no valid person_no values were given, we return an expression which
    # will select no taxa (better than no filter at all).
    
    else
    {
	return "$table.authorizer_no = -1";
    }
}


# ================================================================
# 
# This is here for reference.

# This function takes two taxonomic names -- one from the occurrences/reids
# table and one from the authorities table (broken down in genus (g), 
# subgenus (sg) and species (s) components -- use splitTaxonName to
# do this for entries from the authorities table) and compares
# How closely they match up.  The higher the number, the better the
# match.
# 
# < 30 but > 20 = species level match
# < 20 but > 10 = genus/subgenus level match
# 0 = no match
sub computeMatchLevel {
    my ($occ_g,$occ_sg,$occ_sp,$taxon_g,$taxon_sg,$taxon_sp) = @_;

    my $match_level = 0;
    return 0 if ($occ_g eq '' || $taxon_g eq '');

    if ($taxon_sp) {
        if ($occ_g eq $taxon_g && 
            $occ_sg eq $taxon_sg && 
            $occ_sp eq $taxon_sp) {
            $match_level = 30; # Exact match
        } elsif ($occ_g eq $taxon_g && 
                 $occ_sp && $taxon_sp && $occ_sp eq $taxon_sp) {
            $match_level = 28; # Genus and species match, next best thing
        } elsif ($occ_g eq $taxon_sg && 
                 $occ_sp && $taxon_sp && $occ_sp eq $taxon_sp) {
            $match_level = 27; # The authorities subgenus being used a genus
        } elsif ($occ_sg eq $taxon_g && 
                 $occ_sp && $taxon_sp && $occ_sp eq $taxon_sp) {
            $match_level = 26; # The authorities genus being used as a subgenus
        } elsif ($occ_sg && $taxon_sg && $occ_sg eq $taxon_sg && 
                 $occ_sp && $taxon_sp && $occ_sp eq $taxon_sp) {
            $match_level = 25; # Genus don't match, but subgenus/species does, pretty weak
        } 
    } elsif ($taxon_sg) {
        if ($occ_g eq $taxon_g  &&
            $occ_sg eq $taxon_sg) {
            $match_level = 19; # Genus and subgenus match
        } elsif ($occ_g eq $taxon_sg) {
            $match_level = 17; # The authorities subgenus being used a genus
        } elsif ($occ_sg eq $taxon_g) {
            $match_level = 16; # The authorities genus being used as a subgenus
        } elsif ($occ_sg eq $taxon_sg) {
            $match_level = 14; # Subgenera match up but genera don't, very junky
        }
    } else {
        if ($occ_g eq $taxon_g) {
            $match_level = 18; # Genus matches at least
        } elsif ($occ_sg eq $taxon_g) {
            $match_level = 15; # The authorities genus being used as a subgenus
        }
    }
    return $match_level;
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
# 		SELECT $BASIC_FIELDS,
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


my ($TEMP_TABLE_INDEX) = 1;

sub createTempTable {

    my ($self, $specification) = @_;
    
    my $dbh = $self->{dbh};
    my $success;
    
    my $table_name = "TEMP_TT_$TEMP_TABLE_INDEX"; $TEMP_TABLE_INDEX++;
    
    while (!$success)
    {
	eval {
	    $success = $dbh->do("CREATE TEMPORARY TABLE $table_name ($specification)");
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
    
    return $table_name;
}


# ============================================================================= 
# 
# Accessor routines for objects of type Taxon

package Taxon;

use Carp (qw/carp croak/);

# Universal accessor.  If we can't find the desired field, emit a warning and
# return undefined.

sub get {
    my ($self, $fieldname) = @_;
    
    if ( exists $self->{$fieldname} )
    {
	return $self->{$fieldname};
    }
    else
    {
	carp "could not get field '$fieldname' for Taxon object";
	return;
    }
}


1;

