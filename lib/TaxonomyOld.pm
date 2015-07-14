# 
# The Paleobiology Database
# 
#   Taxonomy.pm
# 

package TaxonomyOld;

#use TaxonTrees;

use Carp qw(carp croak);
use Try::Tiny;

use strict;


our (%TAXONOMIC_RANK) = ( 'max' => 26, 26 => 26, 'informal' => 26, 'unranked_clade' => 25, 'unranked' => 25, 25 => 25,
			 'kingdom' => 23, 23 => 23, 'subkingdom' => 22, 22 => 22,
			 'superphylum' => 21, 21 => 21, 'phylum' => 20, 20 => 20, 'subphylum' => 19, 19 => 19,
			 'superclass' => 18, 18 => 18, 'class' => 17, 17 => 17, 'subclass' => 16, 16 => 16,
			 'infraclass' => 15, 15 => 15, 'superorder' => 14, 14 => 14, 'order' => 13, 13 => 13,
			 'suborder' => 12, 12 => 12, 'infraorder' => 11, 11 => 11, 'superfamily' => 10, 10 => 10,
			 'family' => 9, 9 => 9, 'subfamily' => 8, 8 => 8, 'tribe' => 7, 7 => 7, 'subtribe' => 6, 6 => 6,
			 'genus' => 5, 5 => 5, 'subgenus' => 4, 4 => 4, 'species' => 3, 3 => 3, 'subspecies' => 2, 2 => 2, 'min' => 2 );

our (%RANK_STRING) = ( 26 => 'informal', 25 => 'unranked clade', 23 => 'kingdom', 22 => 'subkingdom', 
		       21 => 'superphylum', 20 => 'phylum', 19 => 'subphylum', 
		       18 => 'superclass', 17 => 'class', 16 => 'subclass', 15 => 'infraclass',
		       14 => 'superorder', 13 => 'order', 12 => 'suborder', 11 => 'infraorder',
		       10 => 'superfamily', 9 => 'family', 8 => 'subfamily', 7 => 'tribe', 6 => 'subtribe',
		       5 => 'genus', 4 => 'subgenus', 3 => 'species', 2 => 'subspecies' );

our (%NOM_CODE) = ( 'iczn' => 1, 'phylocode' => 2, 'icn' => 3, 'icnb' => 4 );

our (%TREE_TABLE_ID) = ( 'taxon_trees' => 1 );

our (%KINGDOM_ALIAS) = ( 'metazoa' => 'Metazoa', 'animalia' => 'Metazoa', 'iczn' => 'Metazoa',
			 'metaphyta' => 'Plantae', 'plantae' => 'Plantae', 'fungi' => 'Fungi',
			 'bacteria' => 'Bacteria', 'eubacteria' => 'Bacteria', 'archaea' => 'Archaea',
			 'protista' => 'Eukaryota', 'chromista' => 'Eukaryota', 'unknown' => 'Eukaryota',
			 'other' => 'Eukaryota' );

our ($SQL_STRING);

our ($IMMPAR_FIELD, $SENPAR_FIELD);

# These need to be synchronized with TaxonTrees.pm, or else moved to a
# separate file that both modules include.

our (@TREE_TABLE_LIST) = ("taxon_trees");

our (%NAME_TABLE) = ("taxon_trees" => "taxon_names");
our (%ATTRS_TABLE) = ("taxon_trees" => "taxon_attrs");
our (%SEARCH_TABLE) = ("taxon_trees" => "taxon_search");
our (%INTS_TABLE) = ("taxon_trees" => "taxon_ints");

our (%AUTH_TABLE) = ("taxon_trees" => "authorities");
our (%OPINION_TABLE) = ("taxon_trees" => "opinions");
our (%OPINION_CACHE) = ("taxon_trees" => "order_opinions");
our (%REFS_TABLE) = ("taxon_trees" => "refs");


=head1 NAME

Taxonomy

=head1 SYNOPSIS

This module is part of the Paleobiology Database Core Application Layer.

An object of this class represents a hierarchically organized set of taxonomic
names.  The set of names known to the database is stored in the C<authorities>
table, with primary key C<taxon_no>.  The primary hierarchy is computed from
the data in the C<authorities> and C<opinions> tables, and is stored in the
table C<taxon_trees>.  This module can be easily modified to define alternate
hierarchies as well, generated using different rules, and to store them in
other tables with the same structure as C<taxon_trees>.  The taxon numbers
from C<authorities> are used extensively as foreign keys throughout the rest
of the database, because the taxonomic hierarchy is central to the
organization of the data.  The hierarchy stored in C<taxon_trees> and related
tables is also referred to extensively throughout the database code, for
example in selecting all of the taxa which are descendents of a base taxon.

=head2 Definitions

The most crucial definition necessary to properly understand this class is the
distinction between I<taxonomic name> and I<taxonomic concept>.  In the course
of this documentation, we will try to use these terms as appropriate; in cases
where the proper term is obvious from context, or is ambiguous, we will simply
use the terms I<taxon> or I<taxa>.

Each distinct taxonomic name/rank combination represented in the database has
a unique entry in the C<authorities> table, and a primary key (taxon_no)
assigned to it in that table.  In the documentation for this database, we use
the term I<taxonomic name> or (alternately I<spelling>) to represent the idea
"distinct taxonomic name/rank combination".  So, for example, "Rhizopodea" as
a class and "Rhizopodea" as a phylum are considered to be distinct spellings
of the same taxonomic concept.  In this case, the taxon's rank was changed at
some point in the past.  It is also the case that "Cyrnaonyx" and "Cyraonyx"
are distinct spellings of the same taxonomic concept; one of these names was
used at some point as a misspelling of the other.

Each distinctly numbered taxonomic name (spelling) is a member of exactly one
taxonomic concept.  Note, however, that taxonomic names are not necessarily
unique.  In a few cases, the same name has been used by different people to
represent different taxonomic concepts.  This is particularly true between
plants and animals, which are covered by entirely different taxonomic
namespaces that in some cases overlap (e.g. Ficus). There are also a few cases
(e.g. Mesocetus) where the general rule of uniqueness within the animal
kingdom has been violated.

A taxonomic hierarchy is built as follows.  For each taxonomic concept in the
database, we algorithmically select a "classification opinion" from among the
entries in the C<opinions> table, representing the most recent and reliable
taxonomic opinion that specifies a relationship between this taxon and the
rest of the taxonomic hierarchy.  These classification opinions are then used
to arrange the taxa into a collection of trees.  Note that the taxa do not
necessarily form a single tree, because there are a number of fossil taxa for
which classification opinions have not yet been entered into the database.
Different taxonomies may use different rules for selecting classification
opinions, or may use different subsets of the C<authorities> and C<opinions>
tables.  This process will be described in more detail below.

=head2 Organization of taxa

The C<authorities> table contains one row for each distinct taxonomic name
(name/rank combination) with C<taxon_no> as primary key.  The C<orig_no> field
associates each row in C<authorities> with the row representing the original
spelling of its taxonomic concept.  Thus, the distinct values of C<orig_no>
identify the distinct taxonomic concepts known to the database.  The
C<taxon_trees> table contains one row for each taxonomic concept, with
C<orig_no> as primary key.

The taxonomic names and concepts are organized according to four separate
relations, based on the data in C<authorities> and C<opinions>.  These
relations are discussed below; the specification in parentheses after each one
indicates the table and field in which the relation is stored.

=over 4

=item Taxonomic concept (authorities:orig_no)

This relation groups together all of the taxonomic names (name/rank
combinations) that represent the same taxonomic concept.  Each row that
represents an original spelling has C<taxon_no = orig_no>.  When a new
spelling for a taxon is encountered, or an opinion is entered which changes
its rank, a new row is created in C<authorities> with the same C<orig_no> but
different C<taxon_no>.

Note that this relation can be taken as an equivalence relation, whereas two
spellings have the same C<orig_no> if and only if they represent the same
taxonomic concept.

=item Accepted name (taxon_trees:spelling_no)

This relation selects from each taxonomic concept the currently accepted
variant (in other words, the currently accepted name/rank combination).  The
value of C<spelling_no> for any concept is the C<taxon_no> corresponding to
the accepted name.  The auxiliary field C<trad_no> records nearly the same
information, but with traditional taxon ranks given precedence over variants
that are ranked as 'unranked clade'.

=item Synonymy (taxon_trees:synonym_no)

This relation indicates for each taxonomic concept the taxonomic concept which
is its most senior synonym.  Two taxa are considered to be synonymous if one
is a subjective or objective synonym of the other, or was replaced by the
other, or if one is an invalid subgroup or nomen dubium, nomen vanum or nomen
nudum inside the other.

The value of C<synonym_no> is the C<orig_no> value of the most senior synonym
for the given concept group.  This means that all concepts which are synonyms
of each other will have the same C<synonym_no> but different C<orig_no>, and
the senior synonym will have C<synonym_no = orig_no>.  This relation can thus
be taken as an equivalence relation, whereas two concepts have the same
C<synonym_no> if and only if they are synonymous.  The set of taxonomic
concepts that share a particular value of C<synonym_no> is called a "synonym
group".

=item Hierarchy (taxon_trees:parent_no)

This relation associates lower with higher taxa.  It forms a collection of
trees, because (as noted above) there are a number of higher fossil taxa for
which no classifying opinion has yet been entered.  Any taxonomic concept for
which no opinion has been entered will have C<parent_no = 0>.

All concepts which are synonyms of each other will have the same C<parent_no>
value.  In computing the hierarchy, we consider all opinions on a synonym
group together.  This relation can also be taken as an equivalence relation,
whereas two taxonomic concepts have the same C<parent_no> if and only if they
are siblings of each other.

=back

=head2 Opinions

In addition to the fields listed above, each entry in C<taxon_trees> (or any
alternative hierarchy tables that may be defined) also has an C<opinion_no>
field.  This field points to the classification opinion that has been
algorithmically selected from the available opinions for that taxon.

For a junior synonym, the value of opinion_no will be the opinion which
specifies its immediately senior synonym.  There may exist synonym chains in
the database, where A is a junior synonym of B which is a junior synonym of C.
In any case, the C<synonym_no> field will always point to the most senior
synonym.

For all taxonomic concepts which are not junior synonyms, the value of
C<opinion_no> will be the opinion which specifies its immediately higher
taxon.  Note that this opinion will also specify a particular spelling of the
higher taxon, which may not be the currently accepted one.  In any case,
C<parent_no> will always point to the original spelling of the parent taxon.

=head2 Tree structure

In order to facilitate tree printouts and logical operations on the taxonomic
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

Several auxiliary tables are needed in order to implement the necessary
functionality for this module.  The tables described here are defined by
default to work with C<taxon_trees>.  If additional hierarchy tables are later
added, then additional instances of each of these tables must be defined as
well.

You will probably not need to refer to these tables directly, but they are
used by the methods defined for this class.

=over 4

=item taxon_search

This table maps strings representing taxonomic names to the corresponding
C<taxon_no> values.  It is needed because a simple search on the C<taxon_name>
field of the C<authorities> table is not sufficiently general.  Using this
table, one can efficiently search for a species name if the full genus name is
not known, or if the known genus is actually a junior synonym of the currently
accepted genus.  Higher taxa can also be found through this table, for full
generality.

=item taxon_names

This table records additional information about each individual taxonomic
name, including its spelling status (i.e. whether it is a misspelling, rank
change, etc.) and the opinion from which this status is taken.

=item taxon_attrs

This table is used to compute hierarchically derived attributes of taxonomic
concepts, such as mass estimates and extancy.

=item suppress_opinions

You will probably never need to refer to this table, but it is included here
for completeness.  This table is needed because the synonymy and hierarchy
relations must be structured as collections of trees.  Unfortunately, the set
of opinions stored in the database may generate cycles in one or both of these
relations.  For example, there will be cases in which the best opinion on
taxon A states that it is a subjective synonym of B, while the best opinion on
taxon B states that it is a subjective synonym of A.  In order to resolve
this, the algorithm that computes the synonymy and hierarchy relations breaks
each cycle by choosing the best (most recent and reliable) opinion from those
that define the cycle and suppressing any opinion that contradicts the chosen
one.  The C<suppress_opinions> table records which opinions are so suppressed.

=back

=head2 Algorithm

The algorithm for building or rebuilding a taxonomic hierarchy is given in the
documentation for C<TaxonTrees.pm>.

=cut

=head1 INTERFACE

I<Note: this is a draft specification, and may change>.

In the following documentation, the parameter C<dbh> is always a database
handle.  Here are some examples:

    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    
    my ($base_taxon) = $taxonomy->getTaxaByName('Conus');
    my $taxon_rank = $base_taxon->{taxon_rank};
    my $reference = Reference->new($dbt, $base_taxon->{reference_no});
    
    my @list = $taxonomy->getTaxa('children', $base_taxon, { return => 'id' });
    my $child_id_list = '(' . join(',', @list) . ')';
    
    my $sth = $dbh->prepare("SELECT some_fields FROM some_table WHERE taxon_no IN $child_id_list");
    
    my @lineage = $taxonomy->getTaxa($base_taxon, 'parents');
    
    my ($id_table) = $taxonomy->getTaxa('all_children', $base_taxon, { return => 'id_table' });
    
    my $result = $dbh->selectall_arrayref("SELECT some_fields FROM $id_table JOIN some_other_table using (taxon_no) WHERE condition");
    
    $dbh->do("DROP TABLE $id_table");

=cut

=head2 Class Methods

=head3 new ( dbh, tree_table_name )

    $taxonomy = Taxonomy->new($dbh, 'taxon_trees');

Creates a new Taxonomy object, which will use the database connection given by
C<dbh> and the taxonomy table named by C<tree_table_name>.  As noted above,
the main taxonomy table is called I<taxon_trees>.  This is currently the only
one defined, but this module and C<TaxonTrees.pm> may at some point be changed
to include others.

=cut

sub new {

    my ($class, $dbh, $table_name) = @_;
    
    croak "unknown tree table '$table_name'" unless $TREE_TABLE_ID{$table_name};
    croak "bad database handle" unless ref $dbh;
    
    # Check for the existence of fields that may have changed.
    
    check_senpar($dbh, $table_name) unless defined $SENPAR_FIELD;
    
    my $self = { dbh => $dbh, 
		 tree_table => $table_name,
		 auth_table => $AUTH_TABLE{$table_name},
		 attrs_table => $ATTRS_TABLE{$table_name},
		 name_table => $NAME_TABLE{$table_name},
		 search_table => $SEARCH_TABLE{$table_name},
		 opinion_table => $OPINION_TABLE{$table_name},
		 opinion_cache => $OPINION_CACHE{$table_name},
		 image_table => 'taxon_images',
	         scratch_table => 'ancestry_scratch' };
    
    bless $self, $class;
    
    return $self;
}


# The following expressions list the various sets of fields that will be
# returned as part of a Taxon or Opinion object:

# The "basic" fields are always returned.

our ($AUTH_BASIC_FIELDS) = "a.taxon_name as exact_name, a.taxon_no, a.taxon_rank, a.common_name, v.is_extant, a.orig_no, o.status, o.parent_no as classification_no, a.reference_no, t.name as taxon_name, t.rank";

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
# They describe how a taxon is linked to others in the tree.

our ($LINK_FIELDS) = ", t.spelling_no, t.synonym_no, t.IMMPAR, t.SENPAR";

# The "orig" fields are returned additionally if we are asked for 'orig'.
# They specify the original name and rank of a taxon.

our ($ORIG_FIELDS) = ", ora.taxon_name as orig_name, ora.taxon_rank as orig_rank";

# The "lft" fields are "lft" and "rgt".  They delineate each taxon's position
# in the hierarchy.

our ($LFT_FIELDS) = ", t.lft, t.rgt";

# The "parent" fields are returned if we are asked for 'parent'.  They
# describe a taxon's immediate parent.

our ($PARENT_FIELDS) = ", pa.taxon_name as parent_name, pa.taxon_rank as parent_rank";

# The "child" fields are returned if we are asked for 'child' (only for
# opinions).  They describe the taxon whose position is assigned by the opinion.

our ($CHILD_FIELDS) = ", ca.taxon_name as child_name, ca.taxon_rank as child_rank";

# The "extant" fields are returned if we are asked for 'extant'.  These fields
# are computed from 'extant' field of the authorities table by propagating
# 'yes' values up the tree and 'no' values down it.  The value of 'is_extant'
# is 1 for extant, 0 for not extant, undefined if not recorded.  The value of
# 'extant_children' is the number of immediate subtaxa which are extant.

our ($EXTANT_FIELDS) = ", v.is_extant, v.extant_children";

# The "type" field is returned if we are asked for 'tt'.  It describes the
# type taxon of the given taxon.

our ($TT_FIELDS) = ", tta.taxon_no as type_taxon_no, tta.taxon_name as type_taxon_name, tta.taxon_rank as type_taxon_rank, a.type_locality";

# The "person" fields are returned if we are asked for 'person'.  They
# indicate the authorizer, enterer and most recent modifier of the given taxon
# or opinion.

our ($PERSON_FIELDS) = ", a.authorizer_no, a.enterer_no, a.modifier_no, pp1.name as authorizer_name, pp2.name as enterer_name, pp3.name as modifier_name";

# The "discussion" fields are returned if we are asked for 'discussion'.

our ($DISCUSSION_FIELDS) = "a.discussion, ppd.name as discussant, ppd.email as discussant_email";

# The "specimen" fields are returned if we are asked for 'specimen', or only
# 'preservation' if we are just asked for that.

our ($SPECIMEN_FIELDS) = ", a.type_specimen, a.type_body_part, a.form_taxon, a.part_details, a.preservation";
our ($PRESERVATION_FIELDS) = ", a.preservation";

# The "pages" fields are returned if we are asked for 'pages'.  They provide
# additional information about the pages on which the taxon was described.

our ($PAGES_FIELDS) = ", a.pages, a.figures";

# The "created" fields are returned if we are asked for 'created'.  They
# indicate the creation and modification date of a record (taxon or opinion).

our ($CREATED_FIELDS) = ", a.created, DATE_FORMAT(a.modified,'%Y-%m-%e %H:%i:%s') modified";

our ($OPINION_CREATED_FIELDS) = ", o.created, DATE_FORMAT(o.modified,'%Y-%m-%e %H:%i:%s') modified";

# The "modshort" fields are returned if we are asked for 'modshort'.

our ($MODSHORT_FIELDS) = ", DATE_FORMAT(a.modified,'%m/%e/%Y') modified_short";

our ($OPINION_MODSHORT_FIELDS) = ", DATE_FORMAT(o.modified,'%m/%e/%Y') modified_short";

# The "comment" fields are returned if we are asked for 'comments'.

our ($COMMENT_FIELDS) = ", a.comments";

# The "size" fields is returned if we are asked for 'size'.  They provide a
# measure of how many subtaxa are contained within the given taxon, both total
# taxa and extant taxa.

our ($SIZE_FIELDS) = ", v.taxon_size as size, v.extant_size as extant_size";

# The "app" fields describe the first and last appearance of the taxon in our
# database.

our ($APP_FIELDS) = ", v.first_early_age as firstapp_ea, v.first_late_age as firstapp_la, v.last_early_age as lastapp_ea, v.last_late_age as lastapp_la";

our ($INT_PHYLO_FIELDS) = ", pi.kingdom_no, pi.kingdom, pi.phylum_no, pi.phylum, pi.class_no, pi.class, pi.order_no, pi.order, pi.family_no, pi.family";

our ($COUNT_PHYLO_FIELDS) = ", pc.phylum_count, pc.class_count, pc.order_count, pc.family_count, pc.genus_count, pc.species_count";

our ($IMG_FIELDS) = ", v.image_no";

our ($REF_BASIC_FIELDS) = "r.reference_no, r.author1init as r_ai1, r.author1last as r_al1, r.author2init as r_ai2, r.author2last as r_al2, r.otherauthors as r_oa, r.pubyr as r_pubyr, r.reftitle as r_reftitle, r.pubtitle as r_pubtitle, r.editors as r_editors, r.pubvol as r_pubvol, r.pubno as r_pubno, r.firstpage as r_fp, r.lastpage as r_lp, r.publication_type as r_pubtype, r.language as r_language, r.doi as r_doi";

# The following hash is used by the return option 'id_table'.

our(%TAXON_FIELD) = ('lft' => 1, 'rgt' => 1, 'depth' => 1, 'opinion_no' => 1,
		     'spelling_no' => 1, 'trad_no' => 1,
		     'synonym_no' => 1, 'parent_no' => 1);



sub check_senpar {
    
    my ($dbh, $table_name) = @_;
    
    my ($senpar_no, $immpar_no);
    
    eval {
	($senpar_no) = $dbh->selectrow_array("SELECT senpar_no FROM $table_name WHERE senpar_no > 0 LIMIT 1");
    };
    
    if ( $senpar_no )
    {
	$SENPAR_FIELD = 'senpar_no';
	$LINK_FIELDS =~ s/SENPAR/senpar_no as parsen_no/;
    }
    
    else
    {
	$SENPAR_FIELD = 'parsen_no';
	$LINK_FIELDS =~ s/SENPAR/parsen_no/;
    }
    
    eval {
	($immpar_no) = $dbh->selectrow_array("SELECT immpar_no FROM $table_name WHERE immpar_no > 0 LIMIT 1");
    };
    
    if ( $immpar_no )
    {
	$IMMPAR_FIELD = 'immpar_no';
	$LINK_FIELDS =~ s/IMMPAR/immpar_no as parent_no/;
    }
    
    else
    {
	$IMMPAR_FIELD = 'parent_no';
	$LINK_FIELDS =~ s/IMMPAR/parent_no/;
    }   
}



=head2 Common options

The methods in this class fall into two main categories.  The first category
includes methods that have the purpose of retrieving taxa from a taxonomic
hierarchy using various criteria.  The second includes auxiliary methods for
treating taxa in various ways.

Each of the methods in the first category can take an "options" hashref as its
last argument.  The options may include one or more of the following, plus any
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

Includes the fields 'spelling_no', 'synonym_no', and 'immpar_no', which
indicate how the taxon is related to the rest of this taxonomic hierarchy.

=item orig

Includes the fields 'orig_name' and 'orig_rank', which indicate the original
name and rank given to this taxonomic concept.

=item lft

Includes the fields 'lft' and 'rgt', which delineates each taxon's position in
the hierarchy.

=item size

Includes the field 'size', which indicates the number of other taxa (valid and
invalid) contained in each taxon.

=item parent

Includes the fields 'parent_name' and 'parent_rank'.  For a taxon, these fields
describe the taxon's parent in the hierarchy.  For opinions, these fields
describe the taxon being assigned to.

=item child

Includes the fields 'child_name' and 'child_rank', which are only valid for
opinions.  These fields describe the taxon being assigned.

=item extant

Includes the field 'is_extant', which is derived from the 'extant' attribute
by propagating 'yes' values up the tree and then 'no' values down it.  The
values are 1 for extant, 0 for not extant, undefined if no value can be
derived.  Also includes 'extant_children', which counts the number of (known)
extant children.

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

=item preservation

Includes just the field 'preservation'.

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
context of a call to getOpinions().

=item return

This option specifies the manner in which the results will be returned.  It
defaults to 'list', unless otherwise stated.  The possible values are:

=over 4

=item list

If this value is specified (or if the 'return' option is not specified at all), the
result will be a list of C<Taxon> or C<Opinion> objects (depending upon which method
was called).

=item hash

If this value is specified, the result will be a hashref whose keys are
taxon_no or opinion_no values corresponding to the found taxa and whose values
are C<Taxon> or C<Opinion> objects.  For example:

   $result = $taxonomy->getTaxa('spellings', $base_taxon_no, { return => 'hash' });

After this call, $result will be a hashref whose keys are taxon_no values
corresponding to each of the various spellings of $base_taxon_no (including
$base_taxon_no itself).  The value of each key will be a Taxon object representing
that taxon.

=item base

If this value is specified, the result will be a hashref whose keys are
taxon_no values corresponding to the specified base taxa and whose values are
lists of Taxon or Opinion objects.  For example:

   $result = $taxonomy->getTaxa('spellings', \@taxon_nos, { return => 'base' };

After this call, $result will be a hashref whose keys are the distinct values
from @taxon_nos.  The value of each key will be a list of Taxon objects
representing the given spellings of that taxon.  Note that in this case two
different lists might in fact be identical, if two different values in
@taxon_nos refer to different spellings of the same taxonomic concept.

=item id

If this value is specified, the result will be a list of distinct taxon_no or
opinion_no values instead of a list of C<Taxon> or C<Opinion> objects.  The 'fields'
option (if any) is ignored in this case.

=item id_table

If this value is specified, the result will be a single string.  This string
will be the name of a temporary table with a column named 'taxon_no'.  This
table will hold the taxon_no values of the selected taxa.  Additional columns
may be specified by an option 'columns', which should be a list of column
names from the tree table, authorities table and/or opinion table.  The option
'columns' is ignored unless 'id_table' is specified.

=item count

If this option is specified, the return value will be a single integer
representing the number of matching taxa.  If filtering options such as
'rank', etc. are avoided, and if 'status' is given as 'all', then this count
is computed from the 'lft' and 'rgt' fields of the C<taxon_trees> table, which is
extremely efficient.

=item stmt

If this option is specified, the return value will be a DBI statement handle
from which the result rows can be read.  This option is only available with
the method C<getTaxa>.

=back

=item rank

This option filters the resulting taxa and returns only those which fall into
the specified taxonomic rank or ranks.  The value of the option can be either
a comma-separated list of ranks or a listref.  Each individual string must be
a single taxonomic rank ('family', 'genus') and may be prefixed by a
comparison operator ('>', '<', '>=', '<=').  All such prefixed clauses are
and'ed together as one clause, and then or'ed together with all of the
individual ones.  Thus, one can select a range of taxa, a list of individual
taxa, or both.

=item kingdom

This option filters the resulting taxa and returns only those which fall into
the specified kingdom of life.  The recognized values are as follows (with
aliases):

=over 4

=item eukaryota

=item metazoa (animalia)

=item metaphytae (plantae)

=item fungi

=item bacteria (eubacteria)

=item archaea

=item other (unknown)

This selects all eukaryota which are not otherwise members of any other kingdom.

=back

You can also specify a nomenclatural code as a value for this option:

=over 4

=item iczn

This selects the metazoa.

=item icn

This selects the metaphytae, fungi, and other.

=back

=item status

This option filters the taxa by their taxonomic and nomenclatural status,
returning only those which match the specified value.  If not specified, it
defaults to 'valid', except as specified below in the documentation of
individual methods.  Recognized values include:

=over 4

=item valid

This value returns only valid taxa, including junior synonyms.

=item senior

This value returns only valid taxa which are not junior synonyms.

=item invalid

This value returns only invalid taxa:  those which are considered to be a
misspelling, nomen dubium, nomen nudum, nomen vanum, invalid subgroup, etc.

=item all

This value returns all taxa.

=back

=item extant

This option filters the taxa by whether or not they are extant.  Recognized
values are 1/yes/true to select extant taxa, 0/no/false to select non-extant taxa,
unknown to select taxa whose status is not known to this database.

=item substitute_senior

This option, if specified, causes the most senior synonym of each matching
taxon to be returned instead of the matching taxon.  The special value
'above_genus' causes this substitution to be done only for taxa above the
genus level (this may be used in order that the genus part of each species
name will always match its reported parent).

=item spelling

This option determines the treatment of taxonomic concepts that include
multiple names.  Depending upon the value of this option, a method may return
one or more records or identifiers for each matching taxonomic concept.  The
possible values are as follows:

=over 4

=item current

This is the default, except as specified below in the documentation for
individual methods.  It returns one taxonomic name for each matching taxonomic
concept, representing the curently accepted spelling of that concept (even if
the currently accepted spelling is not one that matches the other specified
criteria).

=item orig

Returns one taxonomic name for each matching taxonomic concept, representing
the original spelling of that concept (even if the original spelling doesn't
match the other specified criteria).

=item exact

Returns all taxonomic names which exactly match the other specified criteria.

=item all

Returns all taxonomic names for each matching taxonomic concept (even names
which don't match the other specified criteria).

=item limit

If this option is specified, the value must be a positive integer.  The number
of results returned will be no more than this number.  If the value is
undefined, no limit is placed on the results.

=item count

If this option is specified, the SQL query is formulated to allow a result
count to be generated.

=back

=cut

=head2 Object methods

=head3 getTaxon ( taxon_no, options )

Returns a C<Taxon> object corresponding to the given taxon_no.  This is a
convenience routine that calls L</getTaxa> with the relationship C<self>.

=cut

sub getTaxon {

    my ($self, $taxon_no, $options) = @_;
    
    return $self->getRelatedTaxon('self', $taxon_no, $options);
}


=head3 getTaxaByName ( name, options )

Returns a list of C<Taxon> objects representing all concepts in this
taxonomy having one or more spellings which match the given name.  If no
matching taxa are found, returns the empty list.  If called in scalar context,
returns the first taxon found.  The name may include the SQL wildcard
characters % and _.  Any periods appearing in the name are also treated as
wildcards of the % type.

    @taxa = $taxonomy->getTaxaByName('Ficus', { kingdom => 'metazoa' });
    @taxa = $taxonomy->getTaxaByName('F. bernardi');

This method executes a loose match on subspecies, species, subgenus and genus
names, unless the 'exact' option is specified.  Species can be found under
both their genus and their subgenus if any, as well as under any synonymous
genera or subgenera.

The 'name' parameter may be a listref or hashref, in which case the return
value will consist of all taxa whose names exactly match (case insensitively)
any of the list items or hash keys respectively.

Recognized options include:

=over 4

=item fields

Include the specified fields or sets of fields in the returned taxa (see above).

=item return

The resulting taxa will be returned in the specified manner (see above).  The
accepted values for this method are 'list', 'id' and 'hash'.  For the last of
these, the hash keys are the taxonomic names.  The default is 'list'.

=item rank

Only return taxa of the specified rank or ranks (see above).

=item kingdom

Only return taxa from the specified kingdom (see above).  In general, if this
option is specified and the name does not contain wildcards, only one
taxonomic concept should be returned (with a few exceptions, e.g. Mesocetus).
If this option is not specified, then a single name may match more than one
taxonomic concept since the various kingdoms of life do not have distinct
namespaces (e.g. Ficus).

=item status

Only return taxa of the specified status (see above).  Note: for this method,
this option defaults to 'all'.

=item extant

Filter the resulting list of taxa according to whether or not they are extant
(see above).

=item author

Only return taxa that were named by the specified author.

=item pubyr

Only return taxa that were named in the specified year.

=item type_body_part

Only return taxa whose 'type_body_part' attribute matches the specified value.

=item preservation

Only return taxa whose 'preservation' attribute matches the specified value.

=item spelling

Treat matching taxonomic names according to the specified rule (see above).
The accepted values for this method are 'current', 'orig' and 'exact'.

=item substitute_senior

If this option is specified, the most senior synonym of each matching name
will be returned instead (see above).  This option may not be specified along
with C<spelling=exact>.

=item exact

If this option is specified, only exactly matching names will be returned
without respect to case).  In other words, this option suppresses
the "loose match" at the genus level and below.

=item common

If specified, returns taxa whose common name matches the argument as well as
those whose scientific name matches it.  If the value is 'only', only the
common name will be matched.

=item order

If specified, the list of names is ordered by the indicated criterion.  If
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

If this option is not specified, the order is undefined.

=back

=cut

sub getTaxaByName {

    my ($self, $taxon_name, $options) = @_;
    
    # Check the arguments.  Carp a warning and return undefined if name is not
    # defined or is empty.  Names composed of all wildcards are treated empty;
    # if you want to get all of the taxa in the database, you should use
    # getTaxa with the 'all_taxa' parameter.
    
    unless ( defined $taxon_name and $taxon_name ne '' and $taxon_name !~ /^[%_. ]+$/ )
    {
	carp "taxon name is undefined or empty";
	return;
    }
    
    # Set option defaults.
    
    $options ||= {};
    
    my $status = defined $options->{status} && $options->{status} ne '' ?
	lc $options->{status} : 'all';
    my $select = defined $options->{spelling} ? lc $options->{spelling} : 'current';
    my $return = defined $options->{return} ? lc $options->{return} : 'list';
    
    unless ( $select eq 'current' or $select eq 'exact'
	     or $select eq 'orig' or $select eq 'trad' )
    {
	croak "invalid value '$options->{spelling}' for option 'spelling'";
    }
    
    $select = 'trad' if $options->{trad} and $select eq 'current';
    $select = 'spelling' if $select eq 'current';
    
    if ( $options->{substitute_senior} and $select eq 'exact' )
    {
	croak "you may not specify the option 'substitute_senior' together with 'spelling=exact'";
    }
    
    # Construct the appropriate modifiers based on the options.  If we can't
    # parse the name, return undefined.
    
    my (@filter_list, @name_list);
    
    # Determine the name(s) we will be searching for.  Signal an error if we
    # were given a blessed reference (i.e. an object)
    
    if ( ref $taxon_name eq 'ARRAY' )
    {
	@name_list = @$taxon_name;
    }
    
    elsif ( ref $taxon_name eq 'HASH' )
    {
	@name_list = %$taxon_name;
    }
    
    elsif ( ref $taxon_name )
    {
	croak "you may not specify a blessed reference for the parameter 'taxon_name'.";
    }
    
    else
    {
	@name_list = $taxon_name;
    }
    
    # Now we go through each of the names and divide them into the various
    # categories that we know how to search for.  Species and subgenera are
    # grouped under the genus in which they fall, to simplify the resulting
    # expression.
    
    my (@exact, %subgenera, %species);
    
    foreach my $tn (@name_list)
    {
	# If 'common=only' was specified, we take out everything except Roman
	# alphabetic characters, wildcards and single spaces and prepare to
	# search on common_name.
	
	if ( defined $options->{common} && $options->{common} eq 'only' )
	{
	    $tn =~ tr{a-zA-Z%_. }{}cd;
	    $tn =~ s/\./%/g;
	    $tn =~ s/\s+/ /g;
	    $tn =~ s/^\s+//;
	    $tn =~ s/\s+$//;
	    
	    push @exact, $tn;
	}
	
	# Otherwise, we will parse the taxon name and prepare to search by component.
	
	elsif ($tn =~ /^\s*([A-Za-z_.%]+)(?:\s+\(([A-Za-z_.%]+)\))?(?:\s+([A-Za-z_.%]+))?(?:\s+([A-Za-z_.%]+))?/)
	{
	    my $main = $1;
	    my $subgenus = $2 if defined $2;
	    my $species = (defined $4 ? "$3 $4" : $3) if defined $3;
	    
	    $main =~ s/\./%/g;
	    $subgenus =~ s/\./%/g if defined $subgenus;
	    $species =~ s/\./%/g if defined $species;
	    
	    # If the search string specifies a species, then we look for that
	    # name under the genus and subgenus, if any.
	    
	    if ( defined $species )
	    {
		my $context = defined $subgenus ? "$main $subgenus" : $main;
		
		push @{$species{$context}}, $species;
	    }
	    
	    # If we have a subgenus name but not a species name, we search for
	    # the subgenus in the context of its genus.
	    
	    elsif ( defined $subgenus )
	    {
		push @{$subgenera{$main}}, $subgenus;
	    }
	    
	    # Otherwise, we were handed a single taxonomic name, and we don't
	    # know its rank, so we just search for it without any other context.
	    
	    else
	    {
		push @exact, $main;
	    }
	}
	
	else
	{
	    carp "could not interpret '$tn' as a taxonomic name.";
	}
    }
    
    # Now we go through these categories and construct a filter expression.
    
    my (@name_filters, %exact_names, @exact_genera);
    my $name_field = defined $options->{common} && $options->{common} eq 'only' ? 'common_name' : 'taxon_name';
    
    # First, the exact names.  Any that contain wildcards need a separate
    # 'like', otherwise we can just use 'in'.  But we ignore completely any
    # that are composed of only wildcards and spaces.  If someone wants to
    # search for all taxa, they should use getTaxa with the parameter 'all_taxa'.
    
    foreach my $tn (@exact)
    {
	next if $tn =~ /^[%_ ]$/;	# Ignore any entry which is all
                                        # wildcards and spaces
	
	if ( $tn =~ /[%_]/ )
	{
	    if ( $tn =~ / / or $name_field eq 'common_name')
	    {
		push @name_filters, "a.$name_field like '$tn'";
	    }
	    
	    else
	    {
		push @name_filters, "(a.$name_field like '$tn' and a.taxon_rank > 3)";
	    }
	}
	
	else
	{
	    $exact_names{$tn} = 1;
	}
    }
    
    if ( %exact_names )
    {
	my $list = join("','", keys %exact_names);
	push @name_filters, "a.$name_field in ('$list')";
    }
    
    # Then, the subgenera.  Note that we search for subgenera in the context
    # of the specified genus, but also if they don't contain wildcards as
    # genera in their own right.  This is because sometimes subgenera are
    # promoted to genera, and the searcher may not know about this.
    
    foreach my $gn (keys %subgenera)
    {
	my (@clauses, @in_list);
	
	foreach my $sn (@{$subgenera{$gn}})
	{
	    if ( $sn =~ /[%_]/ )
	    {
		push @clauses, "s.taxon_name like '$sn'";
	    }
	    
	    else
	    {
		push @in_list, $sn;
		push @exact_genera, $sn;
	    }
	}
	
	if ( @in_list )
	{
	    my $list = join("','", @in_list);
	    push @clauses, "s.taxon_name in ('$list')";
	}
	
	my $name_clauses = join(' or ', @clauses);
	
	push @name_filters, "(s.genus like '$gn' and ($name_clauses))";
    }
    
    if ( @exact_genera )
    {
	my $list = join("','", @exact_genera);
	push @name_filters, "(s.taxon_rank = 'genus' and s.taxon_name in ('$list'))";
    }
    
    # Then, the species/subspecies.  If both a genus and subgenus are given,
    # we search in the context of either one.
    
    foreach my $gg (keys %species)
    {
	my (@clauses, @in_list);
	
	foreach my $tn (@{$species{$gg}})
	{
	    if ( $tn =~ /[%_]/ )
	    {
		push @clauses, "s.taxon_name like '$tn'";
	    }
	    
	    else
	    {
		push @in_list, $tn;
	    }
	}
	
	if ( @in_list )
	{
	    my $list = join("','", @in_list);
	    push @clauses, "s.taxon_name in ('$list')";
	}
	
	my ($gn, $sn) = split(/ /, $gg);
	
	my $name_clauses = join(' or ', @clauses);
	
	my $context_clause = $sn ?
	    ($gg =~ /[%_]/ ? "(s.genus like '$gn' or s.genus like '$sn')" :
	     "s.genus in ('$gn', '$sn')") :
		 "s.genus like '$gn'";
	
	push @name_filters, "($context_clause and ($name_clauses))";
    }
    
    # If the option 'common' was specified, we also need to look in the
    # common_name field.  If the value given was 'only', then that was already
    # taken care of above.
    
    if ( $options->{common} and $options->{common} ne 'only' )
    {
	my (@in_list);
	
	foreach my $cn ( @exact )
	{
	    if ( $cn =~ /[%_]/ )
	    {
		push @name_filters, "a.common_name like '$cn'";
	    }
	    
	    else
	    {
		push @in_list, $cn;
	    }
	}
	
	if ( @in_list )
	{
	    my $list = join("','", @in_list);
	    push @name_filters, "a.common_name in ('$list')";
	}
    }
    
    # If we couldn't find any names we could understand, return undefined.
    
    return unless @name_filters;
    
    # Finally, construct a single name filter statement.  Unless the option
    # 'loose' was specified, add a clause to make sure that we don't follow
    # loose matches.
    
    my $filter_stmt = join(' or ', @name_filters);
    push @filter_list, "($filter_stmt)";
    
    push @filter_list, "(s.is_exact is null or s.is_exact = 1)" unless $options->{loose};
    
    # Now, set the other filter parameters, plus query fields and tables,
    # based on the specified options.
    
    my $query_fields = $AUTH_BASIC_FIELDS;
    my $extra_tables;
    
    if ( defined $options->{fields} and $return ne 'id' )
    {
	($query_fields, $extra_tables) = $self->generateQueryFields($options->{fields});
	$extra_tables->{v} = 1;
    }
    
    if ( $status ne 'all' )
    {
	push @filter_list, $self->generateStatusFilter('o', $status);
    }
    
    if ( defined $options->{rank} )
    {
	push @filter_list, $self->generateRankFilter('a', $options->{rank});
    }
    
    if ( defined $options->{kingdom} )
    {
	push @filter_list, $self->generateKingdomFilter('t', $options->{kingdom});
    }
    
    if ( defined $options->{extant} )
    {
	$extra_tables->{v} = 1;
	push @filter_list, $self->generateExtantFilter('v', $options->{extant});
    }
    
    if ( defined $options->{author} )
    {
	push @filter_list, $self->generateAuthorFilter('a', $options->{author});
    }
    
    if ( defined $options->{pubyr} )
    {
	push @filter_list, $self->generatePubyrFilter('a', $options->{pubyr}, 'exact');
    }
    
    if ( defined $options->{type_body_part} )
    {
	push @filter_list, $self->generateAttributeFilter('a', 'type_body_part', 'type_body_part',
							  $options->{type_body_part});
    }
    
    if ( defined $options->{preservation} )
    {
	push @filter_list, $self->generateAttributeFilter('a', 'preservation', 'preservation',
							  $options->{preservation});
    }
    
    my $order_expr = '';
    my $limit_expr = '';
    
    if ( defined $options->{order} )
    {
	my $direction = $options->{order} =~ /\.desc$/ ? 'DESC' : 'ASC';
		
	if ( $options->{order} =~ /^size/ )
	{
	    $extra_tables->{v} = 1;
	    $order_expr = "ORDER BY v.taxon_size $direction";
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
    
    if ( defined $options->{limit} )
    {
	my $lim = $options->{limit};
	$lim =~ tr/0-9//dc;
	$limit_expr = "LIMIT " . ($lim + 0);
    }
    
    my $count_expr = $options->{count} ? 'SQL_CALC_FOUND_ROWS' : '';
    
    my $filter_expr = join(' and ', @filter_list);
    my $extra_joins = $self->generateExtraJoins('a', $extra_tables, $select);
    
    # If we are asked to return only taxon_no values, just do that.
    
    if ( $return eq 'id' )
    {
	return $self->getTaxaIdsByName($filter_expr, [], $select, $order_expr, $limit_expr, $count_expr,
				       $extra_joins, $options);
    }
    
    # Otherwise prepare and execute the necessary SQL statement, and return the
    # resulting list.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $search_table = $self->{search_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    if ( $select eq 'exact' )
    {
	$order_expr = "ORDER BY if(o.status in ('belongs to', 'subjective synonym of', 'objective synonym of'), 0, 1), if(a.taxon_no = t.spelling_no, 0, 1)"
	    unless $order_expr;
	
	$SQL_STRING = "
		SELECT $count_expr $query_fields
		FROM $search_table as s JOIN $auth_table as a using (taxon_no)
			JOIN $tree_table as t on t.orig_no = s.orig_no
			LEFT JOIN $opinion_table o using (opinion_no)
			$extra_joins
		WHERE $filter_expr
		$order_expr $limit_expr";
    }
    elsif ( defined $options->{substitute_senior} && lc $options->{substitute_senior} eq 'above_genus' )
    {
	$SQL_STRING = "
		SELECT $count_expr $query_fields
		FROM (SELECT a.taxon_rank, t2.orig_no, t2.synonym_no
		      FROM $search_table as s JOIN $auth_table as a2 using (taxon_no)
				JOIN $tree_table as t2 on t2.orig_no = s.orig_no
				JOIN $auth_table as a on a.taxon_no = t2.spelling_no
				LEFT JOIN $opinion_table as o on o.opinion_no = t2.spelling_no
				$extra_joins
		      WHERE $filter_expr) as ttt
			JOIN $tree_table as t on t.orig_no =
				if(ttt.taxon_rank in ('species', 'subspecies', 'subgenus', 'genus'),
					ttt.orig_no, ttt.synonym_no)
			JOIN $auth_table as a ON a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		GROUP BY a.orig_no $order_expr $limit_expr";
    }
    else
    {
	$order_expr = "ORDER BY if(o.status in ('belongs to', 'subjective synonym of', 'objective synonym of'), 0, 1)"
	    unless $order_expr;
	
	my $join_string = $options->{senior} ?
	    "JOIN $tree_table as t2 on t2.orig_no = s.orig_no JOIN $tree_table as t on t.orig_no = t2.synonym_no" :
		"JOIN $tree_table as t on t.orig_no = s.orig_no";
	
	$SQL_STRING = "
		SELECT $count_expr $query_fields
		FROM $search_table as s JOIN $auth_table as a2 on s.taxon_no = a2.taxon_no
			$join_string
			JOIN $auth_table as a ON a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table o on o.opinion_no = t.opinion_no
			$extra_joins
		WHERE $filter_expr
		GROUP BY a.orig_no $order_expr $limit_expr";
    }
    
    # If we were asked to return a stmt handle, do so.
    
    if ( $return eq 'stmt' )
    {
	my ($stmt) = $dbh->prepare($SQL_STRING);
	$stmt->execute();
	
	return $stmt;
    }
    
    # Otherwise, execute the query and generate a full result list.
    
    my $result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
    
    # If nothing was found, return empty.
    
    unless ( ref $result_list eq 'ARRAY' )
    {
	return {} if $return eq 'hash';
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
	$hashref{$t->{taxon_name}} = $t if $return eq 'hash';
    }
    
    return \%hashref if $return eq 'hash';
    return @$result_list if wantarray;
    return $result_list->[0]; # otherwise
}


sub getTaxaIdsByName {

    my ($self, $filter_expr, $param_list, $select, $order_expr, $limit_expr, $count_expr, $extra_joins, $options) = @_;
    
    # Prepare to fetch the requested information.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $search_table = $self->{search_table};
    my $opinion_table = $self->{opinion_table};
    
    if ( $select eq 'exact' )
    {
	$order_expr = "ORDER BY if(o.status in ('belongs to', 'subjective synonym of', 'objective synonym of'), 0, 1), if(a.taxon_no = t.spelling_no, 0, 1)"
	    unless $order_expr;
	
	$SQL_STRING = "
		SELECT $count_expr a.taxon_no
		FROM $search_table as s JOIN $auth_table as a on s.taxon_no = a.taxon_no
			JOIN $tree_table as t on t.orig_no = s.orig_no
			LEFT JOIN $opinion_table o using (opinion_no)
			$extra_joins
		WHERE $filter_expr
		$order_expr $limit_expr";
    }
    
    elsif ( lc $options->{senior} eq 'above_genus' )
    {
	$SQL_STRING = "
		SELECT $count_expr distinct t.${select}_no
		FROM (SELECT a.taxon_rank, t2.orig_no, t2.synonym_no
		      FROM $search_table as s JOIN $auth_table as a2 on s.match_no = a2.taxon_no
				JOIN $tree_table as t2 on t2.orig_no = s.orig_no
				JOIN $auth_table as a on a.taxon_no = t2.spelling_no
				LEFT JOIN $opinion_table as o on o.opinion_no = t2.opinion_no
				$extra_joins
				$order_expr
		      WHERE $filter_expr) as ttt
			JOIN $tree_table as t on t.orig_no =
				if(ttt.taxon_rank in ('species', 'subspecies', 'subgenus', 'genus'),
					ttt.orig_no, ttt.synonym_no)"
    }
    
    else
    {
	$order_expr = "ORDER BY if(o.status in ('belongs to', 'subjective synonym of', 'objective synonym of'), 0, 1)"
	    unless $order_expr;
	
	my $join_string = $options->{senior} ?
	    "JOIN $tree_table as t2 on t2.orig_no = s.orig_no JOIN $tree_table as t on t.orig_no = t2.synonym_no" :
		"JOIN $tree_table as t on t.orig_no = s.orig_no";
	
	$SQL_STRING = "
		SELECT $count_expr distinct t.${select}_no
		FROM $search_table as s JOIN $auth_table as a on s.match_no = a.taxon_no
			$join_string
			LEFT JOIN $opinion_table o on o.opinion_no = t.opinion_no
			$extra_joins
		WHERE $filter_expr
		$order_expr $limit_expr";
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

Returns a list of Taxon records representing taxa whose names best match the
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

None currently defined.

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
	if ( lc $options->{return} eq 'id' )
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
	return $options->{return} eq 'id' ? $matches->[0]{taxon_no} : $matches->[0];
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

=item fields

Include the specified fields or sets of fields in the returned taxa (see
above). 

=item return

Return the matching taxa in the specified manner (see above).

=item status

Only return taxa with the specified status (see above).

=item extant

Only return taxa that are or are not extant, according to the value of this
option (see above).

=item rank

Only return taxa of the specified rank.  Examples: family, genus.

=item kingdom

Only return taxa from the specified kingdom of life.

=item spelling

Treat matching taxonomic names according to the specified rule (see above).
The accepted values for this method are 'current', 'orig' and 'exact'.  If
this option is not specified, it defaults to 'exact' (which is different from
most of the other methods).

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
    
    my $status = defined $options->{status} && $options->{status} ne '' ? 
	lc $options->{status} : 'valid';
    my $return = defined $options->{return} ? lc $options->{return} : 'list';
    
    unless ( $return eq 'list' or $return eq 'hash' or $return eq 'id' )
    {
	croak "invalid value '$options->{return}' for option 'return'";
    }
    
    my $select = defined $options->{spelling} ? lc $options->{spelling} : 'exact';
    
    unless ( $select eq 'exact' or $select eq 'current' 
	     or $select eq 'orig' or $select eq 'trad' )
    {
	croak "invalid value '$options->{spelling}' for option 'spelling'";
    }
    
    $select = 'trad' if $options->{trad} and $select eq 'current';
    $select = 'spelling' if $select eq 'current';
    
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
    my $extra_tables = {v => 1};
    
    if ( $status ne 'all' )
    {
	push @filter_list, $self->generateStatusFilter('o', $status);
    }
    
    if ( defined $options->{rank} )
    {
	push @filter_list, $self->generateRankFilter('a', $options->{rank});
    }
    
    if ( defined $options->{extant} )
    {
	push @filter_list, $self->generateExtantFilter('v', $options->{extant});
	$extra_tables->{v} = 1;
    }
    
    if ( defined $options->{kingdom} )
    {
	push @filter_list, $self->generateKingdomFilter('t', $options->{kingdom});
    }
    
    if ( defined $options->{fields} and not $return eq 'id' )
    {
	($query_fields, $extra_tables) = $self->generateQueryFields($options->{fields});
	delete $extra_tables->{ref};
    }
    
    my $filter_expr = join(' and ', @filter_list);
    my $extra_joins = $self->generateExtraJoins('a', $extra_tables, $select);
    
    my $count_expr = $options->{count} ? '' : '';
    my $limit_expr = '';
    
    if ( defined $options->{limit} )
    {
	my $lim = $options->{limit};
	$lim =~ tr/0-9//dc;
	$limit_expr = "LIMIT " . ($lim + 0);
    }
    
    # If we are asked to return only taxon_no values, just do that.
    
    if ( $return eq 'id' )
    {
	return $self->getTaxaIdsByReference($filter_expr, \@param_list, $basis, $select, '', $extra_joins);
    }
    
    # Otherwise prepare the necessary SQL statement.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    if ( $basis eq 'authorities' and $select eq 'exact' )
    {
	$SQL_STRING = "
		SELECT $count_expr $query_fields
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			LEFT JOIN refs as r using (reference_no)
			LEFT JOIN $opinion_table as o on t.opinion_no = o.opinion_no
			$extra_joins
		WHERE $filter_expr $limit_expr";
    }
    
    elsif ( $basis eq 'authorities' )
    {
	$SQL_STRING = "
		SELECT $count_expr $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN refs as r on a.reference_no = r.reference_no
			LEFT JOIN $opinion_table as o on t.opinion_no = o.opinion_no
			$extra_joins
		WHERE $filter_expr $limit_expr";
    }
    
    elsif ( $basis eq 'opinions' and $select eq 'exact' )
    {
	$query_fields =~ s/o\.status/if(a.taxon_no=o.child_spelling_no,o.status,null) as status/;
	$query_fields .= ', if(a.taxon_no=o.child_spelling_no,o.parent_spelling_no,null) as parent_spelling_no';
	
	$SQL_STRING = "
		SELECT $count_expr $query_fields
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			JOIN $opinion_table as o on
				(a.taxon_no = o.child_spelling_no or a.taxon_no = o.parent_spelling_no)
			LEFT JOIN refs as r on (a.reference_no = r.reference_no)
			$extra_joins
		WHERE $filter_expr $limit_expr";
    }
    
    elsif ( $basis eq 'opinions' )
    {
	$query_fields =~ s/o\.status/if(a.taxon_no=o.child_spelling_no,o.status,null) as status/;
	$query_fields .= ', if(a.taxon_no=o.child_spelling_no,o.parent_spelling_no,null) as parent_spelling_no';
	
	$SQL_STRING = "
		SELECT $count_expr $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			JOIN $opinion_table as o on
				(a2.taxon_no = o.child_spelling_no or a2.taxon_no = o.parent_spelling_no)
			LEFT JOIN refs as r on (a.reference_no = r.reference_no)
			$extra_joins
		WHERE $filter_expr $limit_expr";
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
	return {} if $return eq 'hash';
	return; # otherwise
    }
    
    # Otherwise, bless all of the objects that we found (if any) into the
    # proper package.  If the option 'hash' was specified, we construct a hash
    # reference.  Otherwise, we return the list.
    
    my %hashref;
    
    foreach my $t (@$result_list)
    {
	bless $t, "Taxon";
	$hashref{$t->{taxon_no}} = $t if $return eq 'hash';
    }
    
    return \%hashref if $return eq 'hash';
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


=head3 getTaxaByOpinions ( relationship, base_taxa, options )

Returns a list of objects of class C<Taxon> representing all taxa under which
(or over which) any of the given taxa have ever been classified.  The entire
set of opinions is searched to determine this result.  If no matching taxa are
found, returns the empty list.

Possible relationships include:

=over 4

=item parents

Returns taxa which have been classified as parents of the specified base
taxon. 

=item children

Returns taxa which have been classified as children of the specified base
taxon.

Options include:

=over 4

=item fields

Include the specified fields or sets of fields in the returned Taxon objects
(see above).

=item return

Return the set of matching taxa in the specified manner (see above).

=item status

Only return taxa for which I<the selecting opinion> gives the specified status.
This is different from how this option works with most of the other methods.

=item rank

Only return taxa of the specified rank or ranks.  Unless the option 'spelling'
is given a value other than 'exact', this filter will only return taxa that
were considered I<in the selecting opinion> to have the specified rank.

=item spelling

Treat matching taxonomic names according to the specified rule (see above).
The accepted values for this method are 'current', 'orig' and 'exact'.  If
this option is not specified, it defaults to 'exact'.

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
    
    my $return = defined $options->{return} ? lc $options->{return} : 'list';
    
    my $status = defined $options->{status} && $options->{status} ne '' ? 
	lc $options->{status} : 'valid';
    my $select = defined $options->{spelling} ? lc $options->{spelling} : 'current';
    
    unless ( $select eq 'current' or $select eq 'orig' or $select eq 'trad'
	     or $select eq 'exact' )
    {
	croak "invalid value '$options->{spelling}' for option 'spelling'";
    }
    
    $select = 'trad' if $options->{trad} and $select eq 'current';
    $select = 'spelling' if $select eq 'current';
    
    # Set filter parameters and query fields based on the specified
    # options.
    
    my (@filter_list);
    
    my $query_fields = $AUTH_BASIC_FIELDS;
    my $extra_tables = {v => 1};
    
    if ( $base_no > 0 )
    {
	push @filter_list, 'a2.taxon_no = ' . ($base_no + 0);
    }
    
    else
    {
	push @filter_list, 'a2.taxon_no in (' . join(',', keys %base_nos) . ')';
    }
    
    if ( $status ne 'all' )
    {
	push @filter_list, $self->generateStatusFilter('o', $status);
    }
    
    if ( defined $options->{rank} )
    {
	push @filter_list, $self->generateRankFilter('a', $options->{rank});
    }
    
    if ( defined $options->{fields} and not $return eq 'id' )
    {
	($query_fields, $extra_tables) = $self->generateQueryFields($options->{fields});
	$extra_tables->{v} = 1;
    }
    
    # Add in 'base_no' if appropriate
    
    unless ( $options->{distinct} )
    {
	$query_fields .= ', a2.taxon_no as base_no';
    }
    
    # Construct the filter expression
    
    my $filter_expr = join(' and ', @filter_list);
    $filter_expr = "WHERE $filter_expr" if $filter_expr ne '';
    my $extra_joins = $self->generateExtraJoins('a', $extra_tables, $select);
    
    # If we were asked for just the taxon_no, do that.
    
    if ( $return eq 'id' )
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
    
    if ( $rel eq 'parents' )
    {
	$anchor = 'child';
	$variable = 'parent';
    }
    
    elsif ( $rel eq 'children' )
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
		SELECT $query_fields, o2.opinion_no as selecting_opinion_no, o2.status as opinion_status, 
			o2.spelling_reason as opinion_spelling_reason
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			JOIN $opinion_table as o2 on a.taxon_no = o2.${variable}_spelling_no
			JOIN $auth_table as a2 on a2.orig_no = o2.${anchor}_no
			LEFT JOIN $opinion_table as o on t.opinion_no = o.opinion_no
			$extra_joins
		$filter_expr";
    }
    
    else
    {
	$SQL_STRING = "
		SELECT $query_fields, o2.opinion_no as selecting_opinion_no, o2.status as opinion_status
		FROM $auth_table as a JOIN $tree_table as t on a.taxon_no = t.${select}_no
			JOIN $opinion_table as o2 on t.orig_no = o2.${variable}_no
			JOIN $auth_table as a2 on a2.orig_no = o2.${anchor}_no
			LEFT JOIN $opinion_table as o on t.opinion_no = o.opinion_no
			$extra_joins
		$filter_expr";
    }
    
    # Execute the SQL statement and return the result list (if there is one).
    
    $result_list = $dbh->selectall_arrayref($SQL_STRING, {});
    
    # If we didn't find any results, return nothing.
    
    unless ( ref $result_list eq 'ARRAY' )
    {
	return {} if $return eq 'hash';
	return; # otherwise
    }
    
    # Otherwise, bless all of the objects that we found (if any) into the
    # proper package.  If the option 'hash' was specified, we construct a hash
    # reference.  Otherwise, we return the list.
    
    my %hashref;
    
    foreach my $t (@$result_list)
    {
	bless $t, "Taxon";
	$hashref{$t->{taxon_no}} = $t if $return eq 'hash';
    }
    
    return \%hashref if $return eq 'hash';
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


=head3 getRelatedTaxon ( relationship, base_taxon, options )

Returns a single Taxon object related in the specified way to the specified
base taxon.  The base taxon can be specified either by a Taxon object or a
taxon number.  The returned object might be the same as the one passed in, for
example if the accepted spelling is requested and the object passed in is
already the accepted spelling for its taxonomic concept.  Possible
relationships are:

=over 4

=item orig

Returns an object representing the original spelling of the base taxon.

=item current

Returns an object representing the currently accepted spelling of the base
taxon.

=item senior

Returns the most senior synonym of the base taxon.

=item parent

Returns the immediate parent of the base taxon.  If you want the senior
synonym of the immediate parent, specify the option 'senior' was well.

=item classification

Returns an object representing the taxon under which the base taxon is
classified.  This will be either the immediate parent or immediate senior
synonym of the base taxon, depending upon its classification opinion.

=item crown_group

Returns an object representing the crown-group within the base taxon.  This
may be the base taxon itself, or it may be one of its included taxa.  The
returned crown group will always be a senior synonym.

=item pan_group

Returns an object representing the pan-group of the base taxon.  This may be
the base taxon itself, or it may be a taxon that includes it.

=back

Available options include:

=over 4

=item return

This option specifies the manner in which the resulting taxon (if any) is
returned.  The only recognized value is 'id', which causes a single taxon_no
value to be returned instead of a C<Taxon> object.

=item senior

If specified, then the most senior synonym of the resulting taxon is returned
instead of the taxon itself.

=item spelling

Treat matching taxonomic names according to the specified rule (see above).
The accepted values for this method are 'current', 'orig' and 'exact'.  The
value 'exact' is only useful for the relationship 'classification'.  If this
option is not specified, it defaults to 'current'.

=back

=cut

sub getRelatedTaxon {
    
    my ($self, $parameter, $base_taxon, $options) = @_;
    
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
    
    if ( defined $base_taxon and $base_taxon =~ /^[0-9]+$/ and 
	 not (defined $base_no and $base_no > 0) )
    {
	$base_no = $base_taxon;
    }
    
    else
    {
	carp "base taxon is undefined or zero";
	return;
    }
    
    # Choose the relation to query on, based on the parameter.
    
    my $rel = lc $parameter;
    
    # Set option defaults.  Some of the relationship choices override the
    # 'spelling' option.
    
    $options ||= {};
    
    my $return = defined $options->{return} ? lc $options->{return} : 'list';
    my $select = defined $options->{spelling} ? lc $options->{spelling} : 'current';
    
    if ( $rel eq 'orig' or $rel eq 'trad' or $rel eq 'current' 
	 or ( $rel eq 'self' and defined $options->{spelling} ) )
    {
	$rel = 'spelling';
	$select = $rel unless $rel eq 'self';
    }
    
    unless ( $select eq 'current' or $select eq 'orig' or $select eq 'trad' or 
	     ($select eq 'exact' and $rel eq 'classification') )
    {
	croak "invalid value '$options->{spelling}' for option 'spelling'";
    }
    
    $select = 'trad' if $options->{trad} and $select eq 'current';
    $select = 'spelling' if $select eq 'current';
    
    my $query_fields = $AUTH_BASIC_FIELDS;
    my $extra_tables = {v => 1};
    
    if ( defined $options->{fields} and $return ne 'id' )
    {
	($query_fields, $extra_tables) = $self->generateQueryFields($options->{fields});
	$extra_tables->{v} = 1;
    }
    
    my $extra_joins = $self->generateExtraJoins('a', $extra_tables, $select);
    
    # If we were asked for just the taxon_no, do that.
    
    return $self->getRelatedTaxonId($base_no, $rel, $select) if $return eq 'id';
    
    # Otherwise, prepare and execute the relevant SQL query
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    my $attrs_table = $self->{attrs_table};
    
    # The relationship 'self' is quite easy to evaluate.  This branch of the
    # code is only evaluated if the 'spelling' option was not given.
    
    if ( $rel eq 'self' )
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		WHERE a.taxon_no = ?";
    }
    
    # The relationship 'spelling' is selected if the user requested a specific
    # spelling of the base taxon.
    
    elsif ( $rel eq 'spelling' )
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		WHERE a2.taxon_no = ?";
    }
    
    # The relationship 'senior' requires an extra join on $tree_table to look
    # up the selected spelling of the senior synonym.
    
    elsif ( $rel eq 'senior' )
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.orig_no = t2.synonym_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		WHERE a2.taxon_no = ?";
    }
    
    # The relationship 'classification' requires an additional use of
    # $opinion_table as well.  If the option 'exact' was specified, then we
    # select the exact taxonomic name used by the classifying opinion.
    
    elsif ( $rel eq 'classification' )
    {
	my $auth_join = $select eq 'exact' ? 
	    "JOIN $auth_table as a on a.taxon_no = o2.parent_spelling_no" :
	    "JOIN $auth_table as a on a.taxon_no = t.${select}_no";
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $opinion_table as o2 using (opinion_no)
			JOIN $tree_table as t on t.orig_no = o2.parent_no
			$auth_join
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		WHERE a2.taxon_no = ?";
    }
    
    # The relationship 'parent' requires a slightly different query if the
    # option 'senior' was also specified.
    
    elsif ( $rel eq 'parent' )
    {
	my $t_join = $options->{senior} ? 
	    "JOIN $tree_table as t3 on t3.orig_no = t2.$IMMPAR_FIELD
	     JOIN $tree_table as t on t.orig_no = t3.synonym_no" :
		"JOIN $tree_table as t on t.orig_no = t2.$IMMPAR_FIELD";
	
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			$t_join
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		WHERE a2.taxon_no = ?";
    }
    
    # The parameter 'crown_group' requires the taxon_attrs table as well.  It
    # should always return the most senior synonym.
    
    elsif ( $rel eq 'crown_group' )
    {
	$SQL_STRING = "
		SELECT $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft >= t2.lft and t.rgt <= t2.rgt
			JOIN $attrs_table as v on v.orig_no = t.orig_no and v.extant_children > 1
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		WHERE a2.taxon_no = ?
		ORDER BY t.depth, if(t.orig_no = t.synonym_no, 0, 1) LIMIT 1";
    }
    
    # The relationship 'pan_group' requires a call to computeAncestry, and
    # then a join on the ancestry_temp table.
    
    elsif ( $rel eq 'pan_group' )
    {
	$self->computeAncestry({ $base_no => 1 });
	
	$SQL_STRING = "
		SELECT $query_fields, v.extant_children as child_count, s.is_base
		FROM $tree_table as t JOIN ancestry_temp as s using (orig_no)
			JOIN $attrs_table as v on v.orig_no = t.orig_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		ORDER BY t.lft DESC";
	
	my $result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
	
	return unless ref $result_list eq 'ARRAY';
	
	# Now go up the list looking for a taxon with more than one extant
	# child (other than the base taxon, which is first in the list).  When
	# we find one, the previous taxon is the pan-group.
	
	my $pan_taxon = $result_list->[0];
	
	foreach my $i (1..$#$result_list)
	{
	    last if $result_list->[$i]{child_count} > 1;
	    $pan_taxon = $result_list->[$i];
	}
	
	bless $pan_taxon, 'Taxon';
	return { $pan_taxon->{taxon_no} => $pan_taxon } if $return eq 'hash';
	return $pan_taxon; # otherwise
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
	
	return { $taxon->{taxon_no} => $taxon } if $return eq 'hash';
	return $taxon; # otherwise
    }
    
    # Otherwise, return empty.
    
    else
    {
	return {} if $return eq 'hash';
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
    
    # Parameters synonym_no and immpar_no require an extra join on $tree_table
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
	my $id_select = $select eq 'exact' ? "o.parent_spelling_no" : "t.${select}_no";
	
	$SQL_STRING = "
		SELECT $id_select
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $opinion_table o using (opinion_no)
			JOIN $tree_table as t on t.orig_no = o.parent_no
		WHERE a2.taxon_no = ?";
    }
    
    # Parameter 'crown' requires the taxon_attrs table as well
    
    elsif ( $rel eq 'crown' )
    {
	my $attrs_table = $self->{attrs_table};
	
	$SQL_STRING = "
		SELECT t.${select}_no
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft >= t2.lft and t.rgt <= t2.rgt
			JOIN $attrs_table as v on v.orig_no = t.orig_no and v.extant_children > 1
		WHERE a2.taxon_no = ?
		ORDER BY t.depth, if(t.orig_no = t.synonym_no, 0, 1) LIMIT 1";
    }
    
    # Parameter 'pan' requires a call to computeAncestry, and then a join on
    # the ancestry_temp table.
    
    elsif ( $rel eq 'pan' )
    {
	my $attrs_table = $self->{attrs_table};
	
	$self->computeAncestry({ $base_no => 1 });
	
	$SQL_STRING = "
		SELECT t.${select}_no as taxon_no, v.extant_children as child_count, s.is_base
		FROM $tree_table as t JOIN ancestry_temp as s using (orig_no)
			JOIN $attrs_table as v on v.orig_no = t.orig_no
		ORDER BY t.lft DESC";
	
	my $result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
	
	return unless ref $result_list eq 'ARRAY';
	
	my $pan_taxon_no;
	
	foreach my $t (@$result_list)
	{
	    last if $t->{child_count} > 1 and not $t->{is_base};
	    $pan_taxon_no = $t->{taxon_no};
	}
	
	return $pan_taxon_no;
    }
    
    else
    {
	croak "invalid relationship '$rel'";
    }
    
    my ($taxon_no) = $dbh->selectrow_array($SQL_STRING, undef, $base_no);

    # Return the taxon number, or undefined if none was found.
    
    return $taxon_no;
}


=head3 getTaxa ( relationship, base_taxa, options )

This is by far the most flexible and useful of all of the methods in this
class.

It returns a list of Taxon objects having the specified relationship to the
specified base taxon or taxa.  If no matching taxa are found, returns an empty
list.  The parameter C<base_taxa> may be either a taxon number or a Taxon
object, an array of either of these, or a hash whose keys are taxon numbers.

In addition to the basic fields that are returned by all of the methods of
this class, the field C<base_no> will be included where appropriate to
indicate which of the base taxa each returned taxon is related to.

Other tables can be joined to the query, by means of the options
'join_tables', 'extra_fields' and 'extra_filters'.

Possible relationships are:

=over 4

=item self

Returns the specified base taxa.  This relationship can be useful for turning
a list of taxon identifiers into C<Taxon> objects.

=item spellings

Returns all of the known taxonomic names associated with the specified base
taxa.  Names which are not represented in any opinion are ignored as spurious.

=item current

Returns the currently accepted spelling of each base taxon.

=item originals

Returns the original spellings of each base taxon.

=item synonyms

Returns the known synonyms (junior and senior) of the specified base taxa.

=item juniors

Returns just the junior synonyms of the specified base taxa.

=item seniors

Returns the most senior synonym of each base taxon.  There is currently no way
to select all more senior synonyms, but that could be added under
'all_seniors' if necessary.

=item classifications

Returns the taxon under which each of the base taxa is classified (which
will be either its immediate parent or immediate senior synonym).

=item parents

Returns the immediate parents of the base taxa.

=item all_parents

Returns all taxa that contain any of the base taxa, all the way up to kingdom
level.

=item common_ancestor

Returns a single taxon which is the most recent common ancestor of the base
taxa.

=item children

Returns a list of objects representing the immediate children of the base
taxon or taxa.

=item all_children

Returns a list of objects representing all the taxa contained within the base
taxon or taxa (all of their descendants).

=item all_taxa

Returns a list of all taxa in the database, filtered by the specified options.
This keyword should be used carefully, as it will return several tens of
megabytes of data unless other constraining options (i.e. pubyr, author,
reference_no, rank) are included as well.  The 'base_taxa' parameter is
ignored, and may be undefined.

=back

Possible options are:

=over 4 

=item return

Specifies the manner in which the matching taxa will be returned.  Recognized
values for this method are: 'list', 'hash', 'base', 'id', 'id_table', 'count'.

=item status

Return only taxa which have the specified status (see above).  It defaults to
'all' for relationship 'spellings', and 'valid' otherwise.

=item extant

Return only taxa whose extancy corresponds to the value of this option (see above).

=item type_body_part

Return only taxa whose 'type_body_part' attribute matches one or more of the
specified values.

=item preservation

Return only taxa whose 'preservation' attribute matches one or more of the
specified values.

=item rank

Returns only taxa which match the specified rank or ranks (see above).

=item kingdom

Returns only taxa which fall into the specified kingdom of life (see above).

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

=item exclude

If specified, then the indicated taxa and all of their children are excluded
from the returned list.  This option is only valid for relationships
'children', 'all_children' and 'all_taxa'.  The value of this option can be
either a single taxon_no value or a comma-separated list, a taxon object, or a
listref, or an object previously generated by C<generateExclusion>.

=item exclude_self

If specified, then the base taxon or taxa are excluded from the returned
list.  By default, they are included.

=item distinct

If this option is specified, only one object will be returned for each
distinct taxon in the result set.  The C<base_no> field will not be included
in any of the returned objects, because it is possible that the same taxon
could have the same relationship (i.e. senior synonym) to more than one member
of the base taxa.

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

If not specified, the order of the returned taxa depends upon the specified
relationship.  For 'spellings', the accepted name will be returned first.  For
'synonyms', the most senior synonym will be returned first.  For the rest, the
taxa will be returned in tree sequence order (which guarantees parents before
children).

=item spelling

Taxonomic names will be treated according to the specified rule (see above).
The accepted values for this method are 'current', 'orig' and 'all'.  If this
option is not specified, it defaults to 'all' for the relationship 'spellings'
and 'current' otherwise.

=item senior

If this is specified, then the most senior synonym of each matching taxon is
returned instead.  This option is only relevant for relationships 'parents',
'all_parents', 'common_ancestor', 'children', 'all_parents', 'all_children',
'all_taxa'.  The special value 'above_genus' causes the most senior synonym to
be only returned for taxa above the genus level.

This option cannot be used in conjunction with spelling => 'exact'.

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
'aragonite'".  To filter on fields in the 'authorities' table, refer to it as
'a', as in 'a.type_locality > 0';

=back

=cut
 
sub getTaxa {
    
    my ($self, $parameter, $base_taxa, $options) = @_;
    
    # Check arguments.  Only throw an error if we were given a reference but
    # couldn't find a taxon number.  If we were passed the undefined value or
    # a taxon number of zero, carp a warning and return undefined.
    
    my $rel = lc $parameter;
    
    my $base_nos;
    
    unless ( $rel eq 'all_taxa' )
    {
	$base_nos = $self->generateBaseNos($base_taxa);
	return unless ref $base_nos and %$base_nos;
    }
    
    # Set option defaults.
    
    my $query_fields = $AUTH_BASIC_FIELDS;
    my $extra_tables = {v => 1};
    
    $options ||= {};
    
    my $return = defined $options->{return} ? lc $options->{return} : 'list';
    
    my $status;
    
    if ( defined $options->{status} and $options->{status} ne '' )
    {
	$status = lc $options->{status};
    }
    
    elsif ( $rel eq 'spellings' or $rel eq 'synonyms' or $rel eq 'juniors' or $rel eq 'seniors' )
    {
	$status = 'all';
    }
    
    else
    {
	$status = 'valid';
    }
    
    my $select = defined $options->{spelling} ? lc $options->{spelling} : 'current';
    
    unless ( $select eq 'current' or $select eq 'orig' or $select eq 'trad' or $select eq 'all'
	     or ($select eq 'exact' and $rel eq 'classifications') )
    {
	croak "invalid value '$options->{spelling}' for option 'spelling'";
    }
    
    $select = 'trad' if $options->{trad} and $select eq 'current';
    $select = 'spelling' if $select eq 'current';
    
    # Set query fields based on the specified options.  This call also tells
    # us which extra tables we will need in order to retrieve those fields.
    # We always include the necessary table to retrieve the parent taxon if
    # the relationship is 'all_parents'.
    
    if ( defined $options->{fields} and $return ne 'id' 
	 and $return ne 'id_table' and $return ne 'count' )
    {
	($query_fields, $extra_tables) = $self->generateQueryFields($options->{fields});
	$extra_tables->{v} = 1;
    }
    
    if ( $rel eq 'all_parents' or $rel eq 'common_ancestor' )
    {
	$extra_tables->{pa} = 1 unless $return eq 'id' or $return eq 'count';
    }
    
    # Set filter clauses based on the specified options
    
    my (@filter_list, @param_list);
    my ($quick_count) = 1;
    
    if ( $rel ne 'all_parents' and $rel ne 'all_taxa' and $rel ne 'common_ancestor' )
    {
	if ( scalar(keys %$base_nos) == 1 )
	{
	    my ($base_no) = keys %$base_nos;
	    push @filter_list, "a2.taxon_no = $base_no";
	}
	
	else
	{
	    push @filter_list, 'a2.taxon_no in (' . join(',', keys %$base_nos) . ')';
	}
    }
    
    if ( $options->{senior} and $rel ne 'parents' and $rel ne 'all_parents' 
	 and $rel ne 'common_ancestor' )
    {
	push @filter_list, "t.orig_no = t.synonym_no";
    }
    
    if ( $options->{exclude_self} )
    {
	if ( $rel eq 'all_parents' )
	{
	    push @filter_list, "not s.is_base";
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
	if ( $rel eq 'children' or $rel eq 'all_children' or $rel eq 'all_taxa' )
	{
	    push @filter_list, $self->generateExcludeFilter('t', $options->{exclude});
	}
	
	else
	{
	    my $exclude_nos = ref $options->{exclude} eq 'Taxon::Exclude' ?
		$options->{exclude}{base_nos} : $self->generateBaseNos($options->{exclude});
	    
	    push @filter_list, 't.orig_no not in (' . join(',', keys %$exclude_nos) . ')'
		if ref $exclude_nos eq 'HASH';
	}
	
	$quick_count = 0;
    }
    
    if ( $status ne 'all' )
    {
	push @filter_list, $self->generateStatusFilter('o', $status);
    }
    
    if ( defined $options->{rank} )
    {
	push @filter_list, $self->generateRankFilter('a', $options->{rank});
	$quick_count = 0;
    }
    
    if ( defined $options->{extant} )
    {
	push @filter_list, $self->generateExtantFilter('v', $options->{extant});
	$extra_tables->{v} = 1;
    }
    
    if ( defined $options->{author} )
    {
	push @filter_list, $self->generateAuthorFilter('a', $options->{author});
	$extra_tables->{ref} = 1;
	$quick_count = 0;
    }
    
    if ( defined $options->{pubyr} )
    {
	push @filter_list, $self->generatePubyrFilter('a', $options->{pubyr},
						      $options->{pubyr_rel});
	$extra_tables->{ref} = 1;
	$quick_count = 0;
    }
    
    if ( defined $options->{type_body_part} )
    {
	push @filter_list, $self->generateAttributeFilter('a', 'type_body_part', 'type_body_part',
							  $options->{type_body_part});
    }
    
    if ( defined $options->{preservation} )
    {
	push @filter_list, $self->generateAttributeFilter('a', 'preservation', 'preservation',
							  $options->{preservation});
    }
    
    if ( defined $options->{created} )
    {
	push @filter_list, $self->generateDateFilter('a.created', $options->{created},
						     $options->{created_rel});
	$quick_count = 0;
    }
    
    if ( defined $options->{modified} )
    {
	push @filter_list, $self->generateDateFilter('a.modified', $options->{modified},
						     $options->{modified_rel});
	$quick_count = 0;
    }
    
    if ( defined $options->{reference_no} )
    {
	push @filter_list, $self->generateRefnoFilter('a', $options->{reference_no});
	$quick_count = 0;
    }
    
    if ( defined $options->{person_no} )
    {
	push @filter_list, $self->generatePersonFilter('a', $options->{person_no},
						       $options->{person_rel});
	$quick_count = 0;
    }
    
    # Select the order in which the results will be returned, as well as the
    # grouping and the limit if any.
    
    my $order_expr = '';
    my $group_expr = '';
    
    if ( defined $options->{order} and $return ne 'count' )
    {
	my $direction = $options->{order} =~ /\.desc$/ ? 'DESC' : 'ASC';
		
	if ( $options->{order} =~ /^size/ )
	{
	    $extra_tables->{v} = 1;
	    $order_expr = "ORDER BY v.taxon_size $direction";
	}
	
	elsif ( $options->{order} =~ /^extant_size/ )
	{
	    $extra_tables->{v} = 1;
	    $order_expr = "ORDER BY v.extant_size $direction";
	}
	
	elsif ( $options->{order} =~ /^extant/ )
	{
	    $extra_tables->{v} = 1;
	    $order_expr = "ORDER BY v.is_extant $direction";
	}
	
	elsif ( $options->{order} =~ /^n_occs/ )
	{
	    $extra_tables->{v} = 1;
	    $order_expr = "ORDER BY v.n_occs $direction";
	}
	
	elsif ( $options->{order} =~ /^name/ )
	{
	    $order_expr = "ORDER BY a.taxon_name $direction";
	}
	
	elsif ( $options->{order} =~ /^lft/ )
	{
	    $order_expr = "ORDER BY t.lft $direction";
	}
	
	elsif ( $options->{order} =~ /^firstapp/ )
	{
	    $extra_tables->{v} = 1;
	    $order_expr = "ORDER BY v.first_early_age $direction";
	}
	
	elsif ( $options->{order} =~ /^lastapp/ )
	{
	    $extra_tables->{v} = 1;
	    $order_expr = "ORDER BY v.last_late_age $direction";
	}
	
	elsif ( $options->{order} =~ /^agespan/ )
	{
	    $extra_tables->{v} = 1;
	    $order_expr = "ORDER BY (v.first_early_age - v.last_late_age) $direction";
	}
	
	elsif ( $options->{order} =~ /^created/ )
	{
	    $order_expr = "ORDER BY a.created $direction";
	}
	
	elsif ( $options->{order} =~ /^created/ )
	{
	    $order_expr = "ORDER BY a.modified $direction";
	}
	
	elsif ( $options->{order} =~ /^pubyr/ )
	{
	    $order_expr = "ORDER BY a.pubyr $direction";
	}
	
	else
	{
	    croak "invalid value '$options->{order}' for option 'order'";
	}
    }
    
    # Add the appropriate grouping expression if necessary
    
    if ( $options->{distinct} )
    {
	if ( $select eq 'all' or $rel eq 'spellings' )
	{
	    $group_expr = 'GROUP BY a.taxon_no';
	}
	else
	{
	    $group_expr = 'GROUP BY a.orig_no';
	}
    }
    
    # And the limit if necessary.
    
    my ($count_expr, $limit_expr) = $self->generateCountLimitExpr($options);
    
    # Add any extra fields and filters that were explicitly specified
    
    if ( defined $options->{extra_fields} and
	 $options->{extra_fields} ne '' )
    {
	$query_fields .= ', ' . $options->{extra_fields};
    }
    
    if ( defined $options->{extra_filters} and
	 $options->{extra_filters} ne '' )
    {
	push @filter_list, (ref $options->{extra_filters} eq 'ARRAY' ? 
			    @{$options->{extra_filters}} : $options->{extra_filters});
	$quick_count = 0;
    }
    
    # Add in 'base_no' if appropriate
    
    unless ( $options->{distinct} or $rel eq 'self' or $rel eq 'all_taxa' 
	     or $rel eq 'all_parents' or $rel eq 'common_ancestor' )
    {
	$query_fields .= ', a2.taxon_no as base_no';
    }
    
    # Compute the necessary expressions to build the query
    
    my $filter_expr = join(' and ', @filter_list);
    $filter_expr = "WHERE $filter_expr" if $filter_expr ne '';
    my $extra_joins = $self->generateExtraJoins('a', $extra_tables, $select, $options);
    
    # Add any extra joins that were explicitly specified
    
    if ( defined $options->{join_tables} )
    {
	my $join_tables = $options->{join_tables};
	
	$join_tables =~ s/\[taxon_no\]/a.taxon_no/g;
	
	$extra_joins .= $join_tables;
    }
    
    # Now, get ready to do the query.  If we were asked for just the count, do
    # that.  If we were asked for just the taxon_nos, do that.
    
    return $self->getTaxaCount($base_nos, $rel, $select, $filter_expr, $extra_joins,
			       $options, $quick_count) if $return eq 'count';
	
    return $self->getTaxaIds($base_nos, $rel, $select, $filter_expr, $extra_joins,
			     $options, $order_expr, $limit_expr) if $return eq 'id' or $return eq 'id_table';
    
    # Otherwise, prepare to execute the relevant SQL query
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $name_table = $self->{name_table};
    my $opinion_table = $self->{opinion_table};
    
    # For parameter 'self', we just select the indicated taxa.  The 'distinct'
    # option is irrelevant here, since only one row is selected for each
    # distinct taxonomic name in the base set.
    
    if ( $rel eq 'self' )
    {
	$filter_expr =~ s/a2\./a\./g;
	
	$SQL_STRING = "
		SELECT $count_expr $query_fields
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		$filter_expr";
    }
    
    # For parameter 'spellings', we need to look at the name table to make sure
    # that we only report spellings that are referenced by child_spelling_no
    # in some opinion.  Any others are spurious and can be ignored.  We also
    # need to make sure to return the currently accepted spelling(s) first.
    
    elsif ( $rel eq 'spellings' )
    {
	$order_expr = "ORDER BY if(n.taxon_no = t.${select}_no, 0, 1)"
	    unless $order_expr;
	
	$SQL_STRING = "
		SELECT $count_expr $query_fields, n.spelling_reason
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o using (opinion_no)
			JOIN $name_table as n on n.orig_no = a2.orig_no
			JOIN $auth_table as a on a.taxon_no = n.taxon_no
			$extra_joins
		$filter_expr
		$group_expr
		$order_expr $limit_expr";
    }
    
    # For parameter 'originals', we select just the original spellings.
    
    elsif ( $rel eq 'originals' )
    {
	$SQL_STRING = "
		SELECT $count_expr $query_fields, n.spelling_reason
		FROM $auth_table as a2 JOIN $auth_table as a on a.taxon_no = a2.orig_no
			JOIN $tree_table as t on t.orig_no = a2.orig_no
			LEFT JOIN $opinion_table as o using (opinion_no)
			LEFT JOIN $name_table as n on n.taxon_no = a2.orig_no
			$extra_joins
		$filter_expr
		$group_expr
		$order_expr $limit_expr";
    }
    
    # For parameter 'current', we select just the current spellings.
    
    elsif ( $rel eq 'current' )
    {
	$SQL_STRING = "
		SELECT $count_expr $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		$filter_expr
		$group_expr
		$order_expr $limit_expr";
    }
    
    # For parameter 'synonyms', we return the most senior synonym(s) first by
    # default.  For 'juniors' we will need some post-processing below in order
    # to separate junior synonyms from senior synonyms.
    
    elsif ( $rel eq 'synonyms' or $rel eq 'juniors' or $rel eq 'seniors' )
    {
	$order_expr = 'ORDER BY if(a.orig_no = t.synonym_no, 0, 1)'
	    if $rel eq 'synonyms' and not $order_expr;
	
	my $synonym_select = $rel eq 'seniors' ? 'orig' : 'synonym';
	
	if ( $select eq 'all' )
	{
	    $SQL_STRING = "
		SELECT $count_expr $query_fields, n.spelling_reason, if(a.orig_no = a2.orig_no, 1, 0) as is_base
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.${synonym_select}_no = t2.synonym_no
			JOIN $name_table as n on n.orig_no = t.orig_no
			JOIN $auth_table as a on a.taxon_no = n.taxon_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$group_expr
		$order_expr $limit_expr";
	}
	else
	{
	    $SQL_STRING = "
		SELECT $count_expr $query_fields, if(a.orig_no = a2.orig_no, 1, 0) as is_base
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.${synonym_select}_no = t2.synonym_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$group_expr
		$order_expr $limit_expr";
	}
    }
    
    # For parameter 'all_taxa', order the results by tree sequence unless
    # otherwise specified.  We need a slightly different query for
    # select='all' than for the others.
    
    elsif ( $rel eq 'all_taxa' )
    {
	$order_expr = 'ORDER BY t.lft' unless $order_expr;
	
	if ( $select eq 'all' )
	{
	    $SQL_STRING = "
		SELECT $count_expr $query_fields, n.spelling_reason
		FROM $auth_table as a JOIN $name_table as n using (taxon_no)
			JOIN $tree_table as t on t.orig_no = n.orig_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$group_expr
		$order_expr $limit_expr";
	}
	
	else
	{
	    $SQL_STRING = "
		SELECT $count_expr $query_fields
		FROM $auth_table as a JOIN $tree_table as t on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$group_expr
		$order_expr $limit_expr";
	}
    }
    
    # For parameter 'children' or 'all_children', order the results by tree
    # sequence unless otherwise specified.
    
    elsif ( $rel eq 'children' or $rel eq 'all_children' )
    {
	my $level_filter = $rel eq 'children' ? 'and t.depth = t2.depth + 1' : '';
	
	$order_expr = 'ORDER BY t.lft'
	    if $order_expr eq '';
	
	if ( $select eq 'all' )
	{
	    $SQL_STRING = "
		SELECT $count_expr $query_fields, n.spelling_reason
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft >= t2.lft and t.lft <= t2.rgt
				$level_filter
			JOIN $name_table as n on n.orig_no = t.orig_no
			JOIN $auth_table as a on a.taxon_no = n.taxon_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$group_expr
		$order_expr $limit_expr";
	    
	}
	
	else
	{
	    $SQL_STRING = "
		SELECT $count_expr $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft >= t2.lft and t.lft <= t2.rgt
				$level_filter
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$group_expr
		$order_expr $limit_expr";
	}
    }
    
    # For parameter 'classifications', do a straightforward lookup using a
    # second copy of the opinion table.
    
    elsif ( $rel eq 'classifications' )
    {
	$SQL_STRING = "
		SELECT $count_expr $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $opinion_table as o2 using (opinion_no)
			JOIN $tree_table as t on t.orig_no = o2.parent_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$group_expr
		$order_expr $limit_expr";
    }
    
    # For parameter 'parents', do a straightforward lookup.  We need a
    # slightly different query if the option 'seniors' was specified.
    
    elsif ( $rel eq 'parents' )
    {
	my $parent_join = $options->{senior} ?
	    "JOIN $tree_table as t3 on t3.orig_no = t2.immpar_no
	     JOIN $tree_table as t on t.orig_no = t3.synonym_no" :
		 "JOIN $tree_table as t on t.orig_no = t2.immpar_no";

	$SQL_STRING = "
		SELECT $count_expr $query_fields
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			$parent_join
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$group_expr
		$order_expr $limit_expr";
    }
    
    # For parameter 'all_parents', we need a more complicated procedure in order to
    # do the query efficiently.  This requires using a scratch table and a
    # stored procedure to recursively fill it in.  The scratch table cannot
    # be a temporary table, due to a limitation of MySQL, so we need to use a
    # global table with locks :(
    
    elsif ( $rel eq 'all_parents' or $rel eq 'common_ancestor' )
    {
	# First select into the temporary table 'ancestry_temp' the set of
	# orig_no values representing the ancestors of the taxa identified by
	# the keys of %$base_nos.
	
	$self->computeAncestry($base_nos);
	
	# Now use this temporary table to do the actual query.
	
	$order_expr = 'ORDER BY t.lft DESC'
	    if $order_expr eq '';
	
	if ( $rel eq 'common_ancestor' )
	{
	    $query_fields .= ', t.lft, t.rgt' unless $query_fields =~ / t\.lft/;
	    $query_fields .= ', s.is_base';
	}
	
	# We need a slightly different query if the option 'senior' was specified.
	
	my $t_join = $options->{senior} ?
	    "$tree_table as t2 JOIN ancestry_temp as s on s.orig_no = t2.orig_no
	     JOIN $tree_table as t on t.orig_no = t2.synonym_no" :
		 "$tree_table as t JOIN ancestry_temp as s on s.orig_no = t.orig_no";
	
	$SQL_STRING = "
		SELECT $count_expr $query_fields, pt.${select}_no as parent_taxon_no
		FROM $t_join
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		GROUP BY t.lft
		$order_expr $limit_expr";
    }
    
    else
    {
	croak "invalid relationship '$parameter'";
    }
    
    # Now execute the indicated query!!!  If we are asked to return a
    # statement handle, do so.
    
    if ( $return eq 'stmt' )
    {
	my ($stmt) = $dbh->prepare($SQL_STRING);
	$stmt->execute();
	
	return $stmt;
    }
    
    # Otherwise, generate a result list.
    
    my $result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
    
    # If we didn't get any results, return nothing.
    
    unless ( ref $result_list eq 'ARRAY' and @$result_list )
    {
	return;
    }
    
    # For some of the relationships, we need to do some post-processing:
    
    # For 'juniors', we must separate junior from senior synonyms.
    
    if ( $rel eq 'juniors' )
    {
	my (%taxon, %is_junior, @juniors);
	
	# First build a hash table of orig_no values.  These correspond to the
	# classification_no values, and thus can be used to follow synonym
	# chains.
	
	foreach my $t (@$result_list)
	{
	    $taxon{$t->{orig_no}} = $t;
	}
	
	# Then, for each taxon number, follow its synonym chain.  If we find a
	# base taxon or known junior synonym, everything in the chain up to that
	# point is a junior synonym.  If not, everything in the chain up to
	# that point is a senior synonym.
	
	foreach my $t (@$result_list)
	{
	    # If we've already decided the status of this taxon, skip to the
	    # next one.
	    
	    next if defined $is_junior{$t->{orig_no}};
	    
	    # Otherwise, follow the classification links until we either find
	    # a base taxon, or a known junior synonym, or the end of the
	    # chain.
	    
	    my @so_far = ($t);
	    my $i = $taxon{$t->{classification_no}};
	    
	    while ( defined $i )
	    {
		last if $i->{is_base} or $is_junior{$i->{orig_no}};
		push @so_far, $i;
		$i = $taxon{$i->{classification_no}};
	    }
	    
	    # If we aren't at the end of the chain, we must have found a base
	    # taxon or known junior synonym.  So everything we have
	    # encountered so far was a junior synonym.  Otherwise, everything
	    # we have encountered so far is known not to be a junior synonym.
	    
	    foreach my $u (@so_far)
	    {
		if ( defined $i )
		{
		    $is_junior{$u->{orig_no}} = 1;
		    push @juniors, $u;
		}
		else
		{
		    $is_junior{$u->{orig_no}} = 0;
		}
	    }
	}
	
	$result_list = \@juniors;
    }
    
    # For 'common_ancestor', we must go through the list and find the most recent
    # common ancestor.
    
    elsif ( $rel eq 'common_ancestor' )
    {
	# First find the minimum and maximum tree sequence values for the base
	# taxa.
	
	my $min;
	my $max = 0;
	my $common_ancestor;
	
	foreach my $t (@$result_list)
	{
	    if ( $t->{is_base} )
	    {
		$min = $t->{lft} if !defined $min or $t->{lft} < $min;
		$max = $t->{lft} if !defined $max or $t->{lft} > $max;
	    }
	}
	
	# Then find the latest taxon which encompasses all of the base taxa.
	
	foreach my $t (@$result_list)
	{
	    $common_ancestor = $t if $t->{lft} <= $min and $t->{rgt} >= $max;
	}
	
	# The result list should be just that taxon.
	
	@$result_list = $common_ancestor;
    }
    
    # Now bless all of the objects that we found (if any) into the proper
    # package.  If the option 'hash' was specified, we construct a hash
    # reference.  Otherwise, we return the list.
    
    my %hashref;
    
    if ( $return eq 'base' and $rel ne 'all_parents' )
    {
	foreach my $t (@$result_list)
	{
	    bless $t, 'Taxon';
	    $hashref{$t->{base_no}} ||= [];
	    push @{$hashref{$t->{base_no}}}, $t;
	}
	
	return \%hashref;
    }
    
    elsif ( $return eq 'hash' or $return eq 'base' )
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


sub getTaxaCount {
    
    my ($self, $base_nos, $rel, $select, $filter_expr, $extra_joins,
        $options, $quick_count) = @_;
    
    # Prepare to fetch the requested information.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    # If $quick_count is true, then we don't have any extra filtering to worry
    # about and can make the count very efficient.
    
    # For parameters 'self' and 'spellings' we do a simple count
    
    if ( $rel eq 'self' )
    {
	$filter_expr =~ s/a2\./a\./g;
	
	if ( $quick_count )
	{
	    $SQL_STRING = "
		SELECT count(distinct a.taxon_no)
		FROM $auth_table as a
		$filter_expr";
	}
	
	else
	{
	    $SQL_STRING = "
		SELECT count(distinct a.taxon_no)
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		$filter_expr";
	}
    }
    
    elsif ( $rel eq 'spellings' )
    {
	if ( $quick_count )
	{
	    $SQL_STRING = "
		SELECT count(distinct a.taxon_no)
		FROM $auth_table as a JOIN $auth_table as a2 using (orig_no)
		$filter_expr";
	}
	
	else
	{
	    $SQL_STRING = "
		SELECT count (distinct a.taxon_no)
		FROM $auth_table as a2 JOIN $auth_table as a using (orig_no)
			JOIN $tree_table as t on t.orig_no = a.orig_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr";
	}
    }
    
    # The count expression for 'synonyms' differs according to whether
    # select=all was specified.  If so, we count all synonymous taxonomic
    # names, otherwise only synonymous concepts.
    
    elsif ( $rel eq 'synonyms' )
    {
	if ( $select eq 'all' )
	{
	    $SQL_STRING = "
		SELECT count(distinct a.taxon_no)
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t using (synonym_no)
			JOIN $auth_table as a on a.orig_no = t.orig_no
		$filter_expr";
	}
	
	elsif ( $quick_count )
	{
	    $SQL_STRING = "
		SELECT count(distinct t.orig_no)
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t using (synonym_no)
		$filter_expr";
	}
	
	else
	{
	    $SQL_STRING = "
		SELECT count(distinct a.taxon_no)
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t using (synonym_no)
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
		$filter_expr";
	}
    }
    
    # For parameter 'all_taxa', the count expresion also differs depending
    # upon whether or not select=all is specified.
    
    elsif ( $rel eq 'all_taxa' )
    {
	if ( $select eq 'all' and $quick_count )
	{
	    $SQL_STRING = "SELECT count(*) FROM $auth_table as a";
	}
	
	elsif ( $select eq 'all' )
	{
	    $SQL_STRING = "
		SELECT count(distinct a.taxon_no)
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr";
	}
	
	elsif ( $quick_count )
	{
	    $SQL_STRING = "SELECT count(*) FROM $tree_table as t";
	}
	
	else
	{
	    $SQL_STRING = "
		SELECT count(distinct t.orig_no)
		FROM $auth_table as a JOIN $tree_table as t on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr";
	}
    }
    
    # for parameter 'all_children', we can get a quick count using the lft and
    # rgt values, as long as $quick_count is true.
    
    elsif ( $rel eq 'all_children' )
    {
	if ( $quick_count and $select ne 'all' )
	{
	    my $exclude = $options->{exclude_self} ? '-1' : '';
	    
	    $SQL_STRING = "
		SELECT sum(rgt-lft$exclude)
		FROM (SELECT t.rgt, t.lft 
			FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
			$filter_expr
			GROUP BY t.orig_no)";
	}
	
	elsif ( $select eq 'all' )
	{
	    $SQL_STRING = "
		SELECT count(distinct a.taxon_no)
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft >= t2.lft and t.lft <= t2.rgt
			JOIN $auth_table as a on a.orig_no = t.orig_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr";
	}
	
	else
	{
	    $SQL_STRING = "
		SELECT count(distinct t.orig_no)
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft >= t2.lft and t.lft <= t2.rgt
			JOIN $auth_table as a on a.orig_no = t.${select}_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr";
	}
    }
    
    # For parameter 'all_parents' we first need to call computeAncestry to get
    # a table holding the ancestors.
    
    elsif ( $rel eq 'all_parents' )
    {
	$self->computeAncestry($base_nos);
	
	my $filter = $options->{exclude_self} ? 'WHERE gen > 1' : '';
	
	$SQL_STRING = "SELECT count(*) from ancestry_temp $filter";
    }
    
    else
    {
	croak "counting taxa for the relationship '$rel' is not implemented";
    }
    
    # Execute the query, and return the result.
    
    my ($count) = $dbh->selectrow_array($SQL_STRING);
    
    return $count;
}


sub getTaxaIds {
    
    my ($self, $base_nos, $rel, $select, $filter_expr, $extra_joins, 
	$options, $order_expr, $limit_expr) = @_;
    
    # Prepare to fetch the requested information.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    my ($taxon_nos);
    
    # We can return one of two things: either a list of taxon_no values, or a
    # table containing them.
    
    my $return_table = 1 if lc $options->{return} eq 'id_table';
    
    # Select extra fields for the new table, if the options 'return =>
    # id_table' and 'fields' were both specified.
    
    my $create_string = '';
    my $select_string = '';
    
    my $field_list = $options->{fields};
    
    if ( defined $field_list and $return_table )
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
	# duplicates.  Whatever was specified will override the default value
	# of $select_string.
	
	foreach my $f ( @$field_list )
	{
	    next if $seen{$f}; $seen{$f} = 1;
	    
	    if ( $f eq 'taxon' )
	    {
		$create_string .= "taxon_no int unsigned not null,\n";
		$select_string .= ",t.${select}_no";
	    }
	    
	    elsif ( $f eq 'current' )
	    {
		$create_string .= "taxon_no int unsigned not null,\n";
		$select_string .= ",t.spelling_no";
	    }
	    
	    elsif ( $f eq 'orig' )
	    {
		$create_string .= "orig_no int unsigned not null,\n";
		$select_string .= ",t.orig_no";
	    }
	    
	    elsif ( $f eq 'senior' )
	    {
		$create_string .= "senior_no int unsigned not null,\n";
		$select_string .= ",ti2.${select}_no";
		$extra_joins .= " JOIN $tree_table as ti2 on ti2.orig_no = t.synonym_no"
		    unless $extra_joins =~ / as t2 /;
	    }
	    
	    elsif ( $f eq 'parent' )
	    {
		$create_string .= "immpar_no int unsigned not null,\n";
		$select_string .= ",ti3.${select}_no";
		$extra_joins .= " JOIN $tree_table as ti3 on ti3.orig_no = t.immpar_no"
		    unless $extra_joins =~ / as t3 /;
	    }
	    
	    elsif ( $f eq 'lft' or $f eq 'rgt' )
	    {
		$create_string .= "$f int unsigned not null,\n";
		$select_string .= ",t.$f";
	    }
	    
	    elsif ( $f eq 'opinion' )
	    {
		$create_string .= "opinion_no int unsigned not null,\n";
		$select_string .= ",t.opinion_no";
	    }
	    
	    else
	    {
		carp "unknown value '$f' for option 'fields'";
	    }
	}
	
	$select_string =~ s/^,//;
	$create_string =~ s/,\s*$//;
    }
    
    # For parameter 'self', we do a simple lookup.
    
    if ( $rel eq 'self' )
    {
	$filter_expr =~ s/a2\./a\./g;
	$select_string = "a.taxon_no" unless $select_string;
	
	$SQL_STRING = "
		SELECT $select_string
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
			LEFT JOIN $opinion_table as o using (opinion_no)
			$extra_joins
		$filter_expr";
    }
    
    # For parameter 'spellings', make sure to return the currently accepted
    # spelling first
    
    elsif ( $rel eq 'spellings' )
    {
	$select_string = "a.taxon_no" unless $select_string;
	$order_expr = "ORDER BY if(a.taxon_no = t.${select}_no, 0, 1)" unless $order_expr;
	
	$SQL_STRING = "
		SELECT $select_string
		FROM $auth_table as a2 JOIN $auth_table as a using (orig_no)
			JOIN $tree_table as t on t.orig_no = a.orig_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr $limit_expr";
    }
    
    # For parameter 'originals', we select the original spellings
    
    elsif ( $rel eq 'originals' )
    {
	$select_string = "a.taxon_no" unless $select_string;
	$order_expr = "ORDER BY t.lft" unless $order_expr;
	
	$SQL_STRING = "
		SELECT DISTINCT $select_string
		FROM $auth_table as a2 JOIN $auth_table as a on a.taxon_no = a2.orig_no
			JOIN $tree_table as t on t.orig_no = a.orig_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr $limit_expr";
    }
    
    # For parameter 'current', we select the current spellings
    
    elsif ( $rel eq 'current' )
    {
	$select_string = "a.taxon_no" unless $select_string;
	$order_expr = "ORDER BY t.lft" unless $order_expr;
	
	$SQL_STRING = "
		SELECT DISTINCT $select_string
		FROM $auth_table as a2 JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr $limit_expr";
    }
    
    # for parameter 'synonyms', make sure to return the most senior synonym first
    
    elsif ( $rel eq 'synonyms' )
    {
	$select_string = "t.${select}_no" unless $select_string;
	$order_expr = "ORDER BY if(t.orig_no = t.synonym_no, 0, 1)" unless $order_expr;
	
	$SQL_STRING = "
		SELECT $select_string
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t using (synonym_no)
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr $limit_expr";
    }
    
    # For parameter 'seniors', we select the senior synonyms
    
    elsif ( $rel eq 'seniors' )
    {
	$select_string = "a.taxon_no" unless $select_string;
	$order_expr = "ORDER BY t.lft" unless $order_expr;
	
	$SQL_STRING = "
		SELECT DISTINCT $select_string
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.orig_no = t2.synonym_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr $limit_expr";
    }
    
    # For parameter 'classifications', do a straightforward lookup using a
    # second copy of the opinion table.
    
    elsif ( $rel eq 'classifications' )
    {
	$select_string = "a.taxon_no" unless $select_string;
	
	$SQL_STRING = "
		SELECT $select_string
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $opinion_table as o2 using (opinion_no)
			JOIN $tree_table as t on t.orig_no = o2.parent_no
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr $limit_expr";
    }
    
    # For parameter 'all_taxa', the first join will differ slightly if select=all
    
    elsif ( $rel eq 'all_taxa' )
    {
	$order_expr = 'ORDER BY t.lft'
	    unless $order_expr;
	
	my $join_expr = $select eq 'all' ? "on a.orig_no = t.orig_no" :
	    "on a.taxon_no = t.${select}_no";
	
	$select_string = "a.taxon_no" unless $select_string;
	
	$SQL_STRING = "
		SELECT $select_string
		FROM $auth_table as a JOIN $tree_table as t $join_expr
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr $limit_expr";
    }
    
    # for parameters 'child' and 'all_children', order the results by tree sequence
    
    elsif ( $rel eq 'children' or $rel eq 'all_children' )
    {
	my $level_filter = $rel eq 'child' ? 'and t.depth = t2.depth + 1' : '';
	
	$order_expr = 'ORDER BY t.lft'
	    unless $order_expr;
	
	my $join_expr = $select eq 'all' ? "on a.orig_no = t.orig_no" :
	    "on a.taxon_no = t.${select}_no";
	
	$select_string = "a.taxon_no" unless $select_string;
	
	$SQL_STRING = "
		SELECT $select_string
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			JOIN $tree_table as t on t.lft >= t2.lft and t.lft <= t2.rgt
				$level_filter
			JOIN $auth_table as a $join_expr
			LEFT JOIN $opinion_table as o ON o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr $limit_expr";
    }
    
    # For parameter 'parents', do a straightforward lookup.  We need a
    # slightly different query of the option 'seniors' was specified.
    
    elsif ( $rel eq 'parents' )
    {
	$order_expr = 'ORDER BY t.lft'
	    unless $order_expr;
	
	my $parent_join = $options->{senior} ?
	    "JOIN $tree_table as t3 on t3.orig_no = t2.immpar_no
	     JOIN $tree_table as t on t.orig_no = t3.synonym_no" :
		 "JOIN $tree_table as t on t.orig_no = t2.immpar_no";
	
	$select_string = "a.taxon_no" unless $select_string;
	
	$SQL_STRING = "
		SELECT $select_string
		FROM $auth_table as a2 JOIN $tree_table as t2 using (orig_no)
			$parent_join
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		$order_expr $limit_expr";
    }
    
    # for parameters 'all_parents' and 'common_ancestor', we need a more
    # complicated procedure in order to do the query efficiently.  This
    # requires using a scratch table and a stored procedure that recursively
    # fills it in.  The scratch table cannot be a temporary table, due to a
    # limitation of MySQL, so we need to use a global table with locks :(
    
    elsif ( $rel eq 'all_parents' or $rel eq 'common_ancestor' )
    {
	# First select into the temporary table 'ancestry_temp' the set of
	# orig_no values representing the ancestors of the taxa identified by
	# the keys of %$base_nos.
	
	$self->computeAncestry($base_nos);
	
	# For 'common_ancestor', we need an extra filter clause derived from
	# the base_no values.
	
	if ( $rel eq 'common_ancestor' )
	{
	    my $base_list = join(',', keys %$base_nos);
	    
	    my ($min, $max) = $dbh->selectrow_array("
		SELECT min(lft), max(lft)
		FROM $tree_table JOIN $auth_table using (orig_no)
		WHERE taxon_no in ($base_list)");
	    
	    $filter_expr .= " and t.lft <= $min and t.rgt >= $max";
	}
	
	# Finally, we can use this scratch table to get the information we
	# need.  We need a slightly different query if the option $senior
	# was specified.
	
	my $t_join = $options->{senior} ?
	    "$tree_table as t2 JOIN ancestry_temp as s on s.orig_no = t2.orig_no
	         JOIN $tree_table as t on t.orig_no = t2.synonym_no" :
		     "$tree_table as t JOIN ancestry_temp as s on s.orig_no = t.orig_no";
	
	$select_string = "t.${select}_no" unless $select_string;
	
	$SQL_STRING = "
		SELECT $select_string
		FROM $t_join
			JOIN $auth_table as a on a.taxon_no = t.${select}_no
			LEFT JOIN $opinion_table as o on o.opinion_no = t.opinion_no
			$extra_joins
		$filter_expr
		ORDER BY t.lft DESC";
    }
    
    else
    {
	croak "invalid relationship '$rel'";
    }
    
    # Execute the query now.  If the option 'return => id_table' was
    # specified, then we insert the results into a temporary table.
    # Otherwise, we return them as a list.
    
    if ( lc $options->{return} eq 'id_table' )
    {
	$create_string = 'taxon_no int unsigned not null' unless $create_string;
	
	my $table_name = $self->createTempTable($create_string);
	
	my $result;
	
	# We need a try block to make sure that we drop the temporary table
	# if an error occurs.
	
	try
	{
	    $result = $dbh->do("INSERT INTO $table_name $SQL_STRING");
	}
	    
	# If an error occurred, we drop the temporary table before re-throwing it.
	
	finally {
	    if ( defined $_[0] )
	    {
		$dbh->do("DROP TABLE IF EXISTS $table_name");
		die $_[0];
	    }
	};
	
	return $table_name;
    }
    
    # Otherwise, we just do the query and return the list of results if any.
    
    else
    {
	$taxon_nos = $dbh->selectcol_arrayref($SQL_STRING);
	
	return unless ref $taxon_nos eq 'ARRAY';
	
	if ( $rel eq 'common_ancestor' )
	{
	    return $taxon_nos->[0];
	}
	
	else
	{
	    return @$taxon_nos;
	}
    }
}


=head3 getTaxonReferences ( relationship, base_taxa, options )

This method returns a list of references associated with the taxa
corresponding to the arguments.  If no matching taxa are found, it returns an
empty list.  The parameter C<base_taxa> may be either a taxon number or a
Taxon object, an array of either of these, or a hash whose keys are taxon
numbers.

In addition to the basic fields that are returned by all of the methods of
this class, the field C<base_no> will be included where appropriate to
indicate which of the base taxa each returned taxon is related to.

Other tables can be joined to the query, by means of the options
'join_tables', 'extra_fields' and 'extra_filters'.

Possible relationships are:

=over 4

=item self

Returns the references associated with the specified base taxa.

=item all_children

Returns a list of objects representing all the taxa contained within the base
taxon or taxa (all of their descendants).

=back

Possible options are:

=over 4 

=item return

Specifies the manner in which the matching taxa will be returned.  Recognized
values for this method are: 'list', 'hash', 'base', 'id', 'id_table', 'count'.

=item select

Specifies which kinds of associated references are returned.  The default
is 'both'.  Possible values include:

=over

=item authority

Returns the references associated with the authority records for the
selected taxa.

=item classification

Returns only the references associated with the classifying opinions for the
selected taxa.

=item both

Returns the references associated with the authority records and/or the
classifying opinions for the selected taxa.  This is the default if the option
is not specified.

=item opinions

Returns the references associated with all opinions about the selected taxa.

=item all

Returns the references associated with the authority records and all opinions
for the selected taxa.

=back

=item status

Return only taxa which have the specified status (see above).  It defaults to
'all' for relationship 'spellings', and 'valid' otherwise.

=item extant

Return only taxa whose extancy corresponds to the value of this option (see above).

=item type_body_part

Return only taxa whose 'type_body_part' attribute matches one or more of the
specified values.

=item preservation

Return only taxa whose 'preservation' attribute matches one or more of the
specified values.

=item rank

Returns only taxa which match the specified rank or ranks (see above).

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

=item exclude

If specified, then the indicated taxa and all of their children are excluded
from the returned list.  This option is only valid for relationships
'children', 'all_children' and 'all_taxa'.  The value of this option can be
either a single taxon_no value or a comma-separated list, a taxon object, or a
listref, or an object previously generated by C<generateExclusion>.

=item exclude_self

If specified, then the base taxon or taxa are excluded from the returned
list.  By default, they are included.

=item order

If specified, the list of references is ordered by the specified criteria.  If
'.desc' is appended to the value, they are ranked in descending order.
Otherwise, they are ranked in ascending order.  Possible values include:

=over 4

=item author

Results are ordered alphabetically by author name.

=item pubyr

Results are ordered by year of publication.

=item rank

Results are ordered by the number of associated taxa.

=item pubtitle

Results are ordered alphabetically by publication title.

=item created

Results are ordered by the date the record was created.

=item modified

Results are ordered by the date the record was last modified.

=back

If not specified, the order of the returned references will be alphabetical by
author name.

=item spelling

Taxonomic names will be treated according to the specified rule (see above).
The accepted values for this method are 'current' and 'all'.  If this
option is not specified, it defaults to 'current'.

=back

=cut
 
sub getTaxonReferences {
    
    my ($self, $parameter, $base_taxa, $options) = @_;
    
    # Check arguments.  Only throw an error if we were given a reference but
    # couldn't find a taxon number.  If we were passed the undefined value or
    # a taxon number of zero, carp a warning and return undefined.
    
    my $rel = lc $parameter;
    
    my $base_nos = $self->generateBaseNos($base_taxa);
    return unless ref $base_nos and %$base_nos;
    
    # Set option defaults.
    
    my $extra_tables = {};
    
    $options ||= {};
    
    my $return = defined $options->{return} ? lc $options->{return} : 'list';
    
    my $status = 'valid';
    
    if ( defined $options->{status} and $options->{status} ne '' )
    {
	$status = lc $options->{status};
    }
    
    else
    {
	$status = 'valid';
    }
    
    my $spelling = defined $options->{spelling} && $options->{spelling} ne '' ? 
	lc $options->{spelling} : 'current';
    
    unless ( $spelling eq 'current' or $spelling eq 'trad' or $spelling eq 'all' )
    {
	croak "invalid value '$options->{spelling}' for option 'spelling'";
    }
    
    $spelling = 'spelling' if $spelling eq 'current';
    
    my $select = defined $options->{select} && $options->{select} ne '' ? 
	lc $options->{select} : 'both';
    
    unless ( $select eq 'authority' or $select eq 'classification' or $select eq 'both'
	     or $select eq 'opinions' or $select eq 'all' )
    {
	croak "invalid value '$options->{select}' for option 'select'";
    }
    
    # Set filter clauses based on the specified options
    
    my (@filter_list, @param_list);
    my ($quick_count) = 1;
    
    if ( scalar(keys %$base_nos) == 1 )
    {
	my ($base_no) = keys %$base_nos;
	push @filter_list, "a2.taxon_no = $base_no";
    }
    
    else
    {
	push @filter_list, 'a2.taxon_no in (' . join(',', keys %$base_nos) . ')';
    }
    
    if ( $options->{exclude_self} && $rel eq 'all_children' )
    {
	push @filter_list, "t.lft != t2.lft";
    }
    
    if ( defined $options->{exclude} && $rel eq 'all_children' )
    {
	push @filter_list, $self->generateExcludeFilter('t', $options->{exclude});
	$quick_count = 0;
    }
    
    if ( $status ne 'all' )
    {
	push @filter_list, $self->generateStatusFilter('o', $status);
    }
    
    if ( defined $options->{rank} )
    {
	push @filter_list, $self->generateRankFilter('a', $options->{rank});
	$quick_count = 0;
    }
    
    if ( defined $options->{extant} )
    {
	push @filter_list, $self->generateExtantFilter('v', $options->{extant});
	$extra_tables->{v} = 1;
    }
    
    if ( defined $options->{author} )
    {
	push @filter_list, $self->generateAuthorFilter('a', $options->{author});
	$quick_count = 0;
    }
    
    if ( defined $options->{pubyr} )
    {
	push @filter_list, $self->generatePubyrFilter('a', $options->{pubyr},
						      $options->{pubyr_rel});
	$quick_count = 0;
    }
    
    if ( defined $options->{type_body_part} )
    {
	push @filter_list, $self->generateAttributeFilter('a', 'type_body_part', 'type_body_part',
							  $options->{type_body_part});
    }
    
    if ( defined $options->{preservation} )
    {
	push @filter_list, $self->generateAttributeFilter('a', 'preservation', 'preservation',
							  $options->{preservation});
    }
    
    if ( defined $options->{created} )
    {
	push @filter_list, $self->generateDateFilter('a.created', $options->{created},
						     $options->{created_rel});
	$quick_count = 0;
    }
    
    if ( defined $options->{modified} )
    {
	push @filter_list, $self->generateDateFilter('a.modified', $options->{modified},
						     $options->{modified_rel});
	$quick_count = 0;
    }
    
    if ( defined $options->{person_no} )
    {
	push @filter_list, $self->generatePersonFilter('a', $options->{person_no},
						       $options->{person_rel});
	$quick_count = 0;
    }
    
    if ( $spelling ne 'all' )
    {
	push @filter_list, 'a.taxon_no = t.spelling_no';
    }
    
    # Select the order in which the results will be returned, as well as the
    # grouping and the limit if any.
    
    my $order_expr = 'ORDER BY r.author1last, r.author1init, r.author2last, r.author2init';
    my $group_expr = 'GROUP BY r.reference_no';
    
    # if ( defined $options->{order} and $return ne 'count' )
    # {
    # 	my $direction = $options->{order} =~ /\.desc$/ ? 'DESC' : 'ASC';
		
    # 	if ( $options->{order} =~ /^size/ )
    # 	{
    # 	    $extra_tables->{v} = 1;
    # 	    $order_expr = "ORDER BY v.taxon_size $direction";
    # 	}
	
    # 	elsif ( $options->{order} =~ /^name/ )
    # 	{
    # 	    $order_expr = "ORDER BY a.taxon_name $direction";
    # 	}
	
    # 	elsif ( $options->{order} =~ /^lft/ )
    # 	{
    # 	    $order_expr = "ORDER BY t.lft $direction";
    # 	}
	
    # 	else
    # 	{
    # 	    croak "invalid value '$options->{order}' for option 'order'";
    # 	}
    # }
    
    # And the limit if necessary.
    
    my ($count_expr, $limit_expr) = $self->generateCountLimitExpr($options);
    
    # Now, get ready to do the query.  We need to compute the various parts of
    # the SQL expression, so that they can be put together below.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $name_table = $self->{name_table};
    my $opinion_cache = $self->{opinion_cache};
    
    my $filter_expr = join(' and ', @filter_list);
    $filter_expr = "WHERE $filter_expr" if $filter_expr ne '';
    
    my $inner_joins;
    my $extra_joins = $self->generateExtraJoins('a', $extra_tables, $spelling, $options);
    
    # If we were asked for just the count, just do that.  If we were asked for
    # just the reference_nos, just do that.  Otherwise, we need the full
    # reference information.
    
    my $query_fields;
    
    if ( $return eq 'count' )
    {
	$query_fields = 'count(distinct reference_no) as count';
	$group_expr = '';
	$order_expr = '';
    }
    
    elsif ( $return eq 'id' or $return eq 'id_table' )
    {
	$query_fields = 'r.reference_no';
    }
    
    else
    {
	$query_fields = $REF_BASIC_FIELDS . ", count(distinct orig_no) as reference_rank";
    }
    
    # For parameter 'self', we just select the references associated with the
    # indicated taxa.  If spelling=current was specified, an appropriate
    # filter was already added above. For parameter 'all_children', we join on
    # taxon_trees twice in order to cover an entire taxonomic subtree.
    
    if ( $rel eq 'self' )
    {
	$filter_expr =~ s/a2\./a\./g;
	$inner_joins = "$auth_table as a JOIN $tree_table as t using (orig_no)";
    }
    
    elsif ( $rel eq 'all_children' )
    {
	$inner_joins = "$auth_table as a JOIN $tree_table as t using (orig_no)
			JOIN $tree_table as t2 on t.lft >= t2.lft and t.lft <= t2.rgt
			JOIN $auth_table as a2 on a2.orig_no = t2.orig_no";
    }
    
    else
    {
	croak "invalid value '$rel' for 'relationship' parameter";
    }
    
    # Now we put the query together depending upon which type of references
    # are being selected.  We start with the "inner query" which retrieves the
    # necessary set of reference_no values along with some columns that
    # indicate what type of reference each one corresponds to.  Below, we will
    # wrap that in an "outer query" which actually retrieves the necessary information.
    
    my $inner_query;
    
    if ( $select eq 'authority'	)
    {
	$inner_query = "SELECT a.reference_no, t.orig_no
		FROM $inner_joins
			LEFT JOIN $opinion_cache as o on o.opinion_no = t.opinion_no
		$filter_expr";
	$query_fields .= ", 1 as is_auth";
    }
    
    elsif ( $select eq 'classification' )
    {
	$inner_query = "SELECT o.reference_no, t.orig_no
		FROM $inner_joins
			JOIN $opinion_cache as o on o.opinion_no = t.opinion_no
		$filter_expr";
	$query_fields .= ", 1 as is_class";
    }
    
    elsif ( $select eq 'opinions' )
    {
	$inner_query = "SELECT o.reference_no, t.orig_no, 1 as is_class
		FROM $inner_joins
			JOIN $opinion_cache as o on o.opinion_no = t.opinion_no
		$filter_expr
		UNION
		SELECT oa.reference_no, a.orig_no, 0 as is_class
		FROM $inner_joins
			LEFT JOIN $opinion_cache as o on o.opinion_no = t.opinion_no
			JOIN $opinion_cache as oa on oa.child_spelling_no = a.taxon_no
		$filter_expr";
	$query_fields .= ", sum(is_class) as is_class, 1 as is_opinion";
    }
    
    elsif ( $select eq 'both' )
    {
	$inner_query = "SELECT a.reference_no, t.orig_no, 1 as is_auth, 0 as is_class
		FROM $inner_joins
			LEFT JOIN $opinion_cache as o on o.opinion_no = t.opinion_no
		$filter_expr
		UNION
		SELECT o.reference_no, t.orig_no, 0 as is_auth, 1 as is_class
		FROM $inner_joins
			JOIN $opinion_cache as o on o.opinion_no = t.opinion_no
		$filter_expr";
	$query_fields .= ", sum(is_auth) as is_auth, sum(is_class) as is_class";
    }
    
    elsif ( $select eq 'all' )
    {
	$inner_query = "SELECT a.reference_no, t.orig_no, 1 as is_auth, 0 as is_class, 0 as is_opinion
		FROM $inner_joins
			LEFT JOIN $opinion_cache as o on o.opinion_no = t.opinion_no
		$filter_expr
		UNION
		SELECT o.reference_no, t.orig_no, 0 as is_auth, 1 as is_class, 1 as is_opinion
		FROM $inner_joins
			JOIN $opinion_cache as o on o.opinion_no = t.opinion_no
		$filter_expr
		UNION
		SELECT oa.reference_no, a.orig_no, 0 as is_auth, 0 as is_class, 1 as is_opinion
		FROM $inner_joins
			LEFT JOIN $opinion_cache as o on o.opinion_no = t.opinion_no
			JOIN $opinion_cache as oa on oa.child_spelling_no = a.taxon_no
		$filter_expr";
	$query_fields .= ", sum(is_auth) as is_auth, sum(is_class) as is_class, sum(is_opinion) as is_opinion";
    }
    
    else
    {
	croak "unrecognized value '$select' for option 'select'";
    }
    
    # Now construct the full query using what we constructed above as a subquery.
    
    $SQL_STRING = "
	SELECT $count_expr $query_fields
	FROM refs as r JOIN
	($inner_query) as s where r.reference_no = s.reference_no
	$group_expr $order_expr $limit_expr";
    
    # Then execute the query!!!  If we are asked to return a
    # statement handle, do so.
    # print STDERR $SQL_STRING . "\n\n";
    if ( $return eq 'stmt' )
    {
	my ($stmt) = $dbh->prepare($SQL_STRING);
	$stmt->execute();
	
	return $stmt;
    }
    
    # Otherwise, generate a result list.
    #print STDERR $SQL_STRING . "\n\n";
    
    my $result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
    
    # If we didn't get any results, return nothing.
    
    unless ( ref $result_list eq 'ARRAY' and @$result_list )
    {
	return;
    }
    
    # If we are returning a result list, bless all of the objects into the
    
    my %hashref;
    
    if ( $return eq 'hash' or $return eq 'base' )
    {
	foreach my $t (@$result_list)
	{
	    bless $t, 'Reference';
	    $hashref{$t->{taxon_no}} = $t;
	}
	
	return \%hashref;
    }
    
    else
    {
	foreach my $t (@$result_list)
	{
	    bless $t, 'Reference';
	}
	
	return @$result_list;
    }
}


=head3 getOpinions ( relationship, base_taxa, options )

Returns a list of C<Opinion> objects having the specified relationship to the
specified base taxon or taxa.  If no matching opinions are found, returns an
empty list.  The parameter C<base_taxa> may be either a taxon number or a Taxon
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
case, the parameter C<base_taxa> is ignored and may be undefined.  This
version of the call should be used with caution, as it could return a very
large number (in the hundreds of thousands) of records unless other filtering
options are also specified.

=back

Possible options are:

=over 4

=item return

Specifies the manner in which the results will be returned (see above).
Recognized values for this method are 'id', 'list', 'hash'.  In the latter
case, the hash keys are opinion_no values.

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

    my ($self, $parameter, $base_taxa, $options) = @_;
    
    # Check arguments.  Only throw an error if we were given a reference but
    # couldn't find a taxon number.  If we were passed the undefined value or
    # a taxon number of zero, carp a warning and return undefined.
    
    my $rel = lc $parameter;
    
    my $base_nos;
    
    unless ( $rel eq 'all_opinions' )
    {
	$base_nos = $self->generateBaseNos($base_taxa);
	return unless ref $base_nos and %$base_nos > 0;
    }
    
    # Set option defaults.
    
    my $query_fields = $OPINION_BASIC_FIELDS;
    my $extra_tables = {};
    
    $options ||= {};
    
    my $return = defined $options->{return} ? lc $options->{return} : 'id';
    
    my $status = defined $options->{status} && $options->{status} ne '' ?
	lc $options->{status} : 'all';
    
    # Set query fields based on the specified options.
    
    if ( defined $options->{fields} and $return ne 'id' )
    {
	($query_fields, $extra_tables) = $self->generateOpinionQueryFields($options->{fields});
    }
    
    # Set filter clauses based on the specified options
    
    my (@filter_list, @param_list);
    
    my ($taxon_expr, $join_expr);
    my ($tree_join_expr) = '';
    
    if ( scalar(keys %$base_nos) == 1 )
    {
	my ($base_no) = keys %$base_nos;
	$taxon_expr = '= ' . $base_no;
    }
    
    else
    {
	$taxon_expr = 'in (' . join(',', keys %$base_nos) . ')';
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
    
    if ( $return eq 'id' )
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
		GROUP BY opinion_no ASC
		ORDER BY ri DESC, pubyr DESC, opinion_no DESC";
    }
    
    $result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
    
    # If we didn't get any results, return nothing.
    
    unless ( ref $result_list eq 'ARRAY' )
    {
	return $return eq 'hash' ? {} : ();
    }
    
    # Otherwise, bless all of the objects that we found (if any) into the
    # proper package.  If the option 'hash' was specified, we construct a hash
    # reference.  Otherwise, we return the list.
    
    my %hashref;
    
    foreach my $t (@$result_list)
    {
	bless $t, "Opinion";
	$hashref{$t->{opinion_no}} = $t if $return eq 'hash';
    }
    
    return \%hashref if $return eq 'hash';
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


=head3 generateExclusion ( excluded_taxa )

Returns an object of class C<Taxon::Exclusion>, which can then be passed to
C<getTaxa> in order to exclude the specified taxa.  This routine allows us to
do the work of calculating the exclusion expression once, and then use it in
multiple queries.

=cut

sub generateExclusion {
    
    my ($self, $base_taxa) = @_;
    
    # First get the base taxon_no values.  Return empty if none are found.
    
    my $base_nos = $self->generateBaseNos($base_taxa);
    return unless ref $base_nos and %$base_nos > 0;
    
    # Then get the lft and rgt values for each taxon.
    
    my $dbh = $self->{dbh};
    my $tree_table = $self->{tree_table};
    my $auth_table = $self->{auth_table};
    my $taxon_list = join(',', keys %$base_nos);
    
    $SQL_STRING = "
		SELECT distinct t.lft, t.rgt
		FROM $tree_table as t JOIN $auth_table as a using (orig_no)
		WHERE a.taxon_no in ($taxon_list)";
    
    my $result_list = $dbh->selectall_arrayref($SQL_STRING, { Slice => {} });
    
    return unless ref $result_list eq 'ARRAY';
    
    # Then generate a set of filter clauses
    
    my @filter_clauses;
    
    foreach my $row (@$result_list)
    {
	next unless $row->{lft} > 0 and $row->{rgt} > 0;
	
	push @filter_clauses, "t.lft not between $row->{lft} and $row->{rgt}";
    }
    
    return unless @filter_clauses;
    
    my $exclude_string = '(' . join(' and ', @filter_clauses) . ')';
    
    # Then package them up into a Taxon::Exclusion object
    
    my $result = { base_nos => $base_nos, filter_string => $exclude_string };
    
    bless $result, 'Taxon::Exclude';
    return $result;
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
    
    # Query all children to check whether this list includes at least one
    # genus or species-level entry.  If so, then this name is still used.
    
    my $dbh = $self->{dbh};
    my $auth_table = $self->{auth_table};
    my $tree_table = $self->{tree_table};
    
    my ($taxon_count) = $dbh->selectrow_array("
	SELECT count(*)
	FROM $auth_table as a JOIN $tree_table as t using (orig_no)
		JOIN $tree_table as t2 on t.lft > t2.lft and t.lft <= t2.rgt
		JOIN $auth_table as a2 on t2.orig_no = a2.orig_no
	WHERE a2.taxon_no = $taxon_no AND a.taxon_rank in ('genus', 'subgenus', 'species')");
    
    # If we found something, return true (i.e. the taxon is used) and
    # otherwise return false.
    
    return $taxon_count > 0;
}


=head3 getClassOrderFamily ( focal_taxon, parent_list )

Using the given list of parent Taxon objects, modify the given Taxon object to
add fields 'class', 'order', 'family' and 'common_name' if it doesn't already
have one.  Also add 'category'.  Also return a list the class, order,
and family names.

=cut

# JA: started another heavy rewrite 26.9.11, finished it 25.10.11
# MM: modified the argument format, but didn't change the algorithm 2012-11-29

sub getClassOrderFamily {
    
    my ($self, $focal_taxon, $parent_list) = @_;
    
    my $dbh = $self->{dbh};
    my $auth_table = $self->{auth_table};
    my $opinion_table = $self->{opinion_table};
    
    # Make sure we have a list of parents to work from.
    
    return unless ref $parent_list eq 'ARRAY' and @$parent_list > 0;
    
    # Keep track of the boundary in the list between what is still under
    # consideration and what we are done with.
    
    my ($boundary) = 0;
    
    # Common name and family are easy.  We traverse the list from lowest rank
    # to highest (i.e. in reverse order).
    
    foreach my $i (0..$#$parent_list)
    {
	my $t = $parent_list->[$i];
	
	last if $t->{'taxon_rank'} =~ /superclass|phylum|kingdom/;
	
	if ( ! $focal_taxon->{common_name} && $t->{common_name} )
	{
	    $focal_taxon->{common_name} = $t->{common_name};
	}
	
	if ( ( $t->{'taxon_rank'} eq "family" || $t->{'taxon_name'} =~ /idae$/ ) && ! $t->{'family'} )
	{
	    $focal_taxon->{'family'} = $t->{'taxon_name'};
	    $focal_taxon->{'family_no'} = $t->{'taxon_no'};
	}
	
	# The topmost family/tribe/genus/species entry will now mark the
	# boundary between what we're done with and what we still need to consider.
	
	if ( $t->{'taxon_rank'} =~ /family|tribe|genus|species/ && $t->{'taxon_rank'} ne "superfamily" )
	{
	    $boundary = $i + 1;
	}
    }
    
    # We need to know which parents have ever been ranked as either a class
    # or an order
    
    my (@other_parent_nos,%wasClass,%wasntClass,%wasOrder,%wasntOrder);
    
    # First mark names currently ranked at these levels.

    foreach my $i ($boundary..$#$parent_list)
    {
	my $t = $parent_list->[$i];
	my $orig_no = $t->{taxon_no};
	
	# used by jsonCollection 30.6.12
	if ( ! $focal_taxon->{'category'} )
	{
	    if ( $t->{taxon_name} =~ /Vertebrata|Chordata/ ) {
		$focal_taxon->{'category'} = "vertebrate";
	    } elsif ( $t->{taxon_name} =~ /Insecta/ ) {
		$focal_taxon->{'category'} = "insect";
	    } elsif ( $t->{taxon_name} =~ /Animalia|Metazoa/ ) {
		$focal_taxon->{'category'} = "invertebrate";
	    } elsif ( $t->{taxon_name} eq "Plantae" ) {
		$focal_taxon->{'category'} = "plant";
	    }
	}
	
	if ( $t->{taxon_rank} eq "class" )
	{
	    $wasClass{$orig_no} = 9999;
	} elsif ( $t->{taxon_rank} eq "order" )	{
	    $wasOrder{$orig_no} = 9999;
	} elsif ( $orig_no )	{
	    push @other_parent_nos, $orig_no;
	}
    }
    
    $focal_taxon->{category} ||= "microfossil";
    
    # Then find other names previously ranked at these levels.
    
    if ( @other_parent_nos )
    {
	my $other_list = join(',', @other_parent_nos);
	
	my $sql = "
		SELECT a.taxon_rank, a.orig_no, count(*) as count
		FROM $auth_table as a JOIN $opinion_table as o on a.taxon_no = o.child_spelling_no
		WHERE o.child_no in ($other_list)
		GROUP BY a.taxon_no";
	
	my $result_list = $dbh->selectall_arrayref($sql, { Slice => {} }) || [];
	
	foreach my $p (@$result_list)
	{
	    if ( $p->{taxon_rank} eq "class" )	{
		$wasClass{$p->{orig_no}} += $p->{'c'};
	    } else	{
		$wasntClass{$p->{orig_no}} += $p->{'c'};
	    }
	    if ( $p->{taxon_rank} eq "order" )	{
		$wasOrder{$p->{orig_no}} += $p->{'c'};
	    } else	{
		$wasntOrder{$p->{orig_no}} += $p->{'c'};
	    }
	}
    }
    
    # Find the oldest parent most frequently ranked an order.  Use publication
    # year as a tie breaker.
    
    my ($maxyr,$mostoften,$orderlevel) = ('',-9999,'');
    
    foreach my $i ($boundary..$#$parent_list)
    {
	my $t = $parent_list->[$i];
	my $t_no = $t->{orig_no};
	
	last if $wasClass{$t_no} > 0 || $t->{'taxon_rank'} =~ /phylum|kingdom/;
	
	if ( ( $wasOrder{$t_no} - $wasntOrder{$t_no} > $mostoften && $wasOrder{$t_no} > 0 ) || ( $wasOrder{$t_no} - $wasntOrder{$t_no} == $mostoften && $wasOrder{$t_no} > 0 && $t->{'pubyr'} < $maxyr ) )
	{
	    $mostoften = $wasOrder{$t_no} - $wasntOrder{$t_no};
	    $maxyr = $t->{pubyr};
	    $focal_taxon->{order} = $t->{taxon_name};
	    $focal_taxon->{order_no} = $t->{taxon_no};
	    $boundary = $i + 1;
	}
    }
    
    # If that fails then none of the parents have ever been orders, so use the
    # oldest name between the levels of family and at-least-once class.
    
    unless ( $focal_taxon->{order_no} )
    {
	foreach my $i ($boundary..$#$parent_list)
	{
	    my $t = $parent_list->[$i];
	    my $t_no = $t->{orig_no};
	    
	    last if $wasClass{$t_no} > 0 || $t->{'taxon_rank'} =~ /phylum|kingdom/;
	    
	    if ( ! $maxyr || $t->{'pubyr'} < $maxyr )
	    {
		$maxyr = $t->{'pubyr'};
		$focal_taxon->{'order'} = $t->{'taxon_name'};
		$focal_taxon->{'order_no'} = $t->{taxon_no};
		$boundary = $i + 1;
	    }
	}
    }
    
    # Now find the oldest parent ever ranked as a class.
    
    ($maxyr,$mostoften) = ('',-9999);
    
    foreach my $i ($boundary..$#$parent_list)
    {
	my $t = $parent_list->[$i];
	my $t_no = $t->{orig_no};
	
	if ( ( $wasClass{$t_no} - $wasntClass{$t_no} > $mostoften && $wasClass{$t_no} > 0 ) || ( $wasClass{$t_no} - $wasntClass{$t_no} == $mostoften && $wasClass{$t_no} > 0 && $t->{'pubyr'} < $maxyr ) )	{
	    $mostoften = $wasClass{$t_no} - $wasntClass{$t_no};
	    $maxyr = $t->{'pubyr'};
	    $focal_taxon->{'class'} = $t->{'taxon_name'};
	    $focal_taxon->{'class_no'} = $t->{taxon_no};
	}
    }
    
    # Otherwise we're really in trouble, so use the oldest name available.
    
    unless ( $focal_taxon->{class_no} )
    {
	for (my $i = $boundary; $i >= 0; $i--)
	{
	    my $t = $parent_list->[$i];
	    my $t_no = $t->{orig_no};
	    
	    last if $t->{taxon_rank} =~ /phylum|kingdom/;
	    
	    if ( ! $maxyr || $t->{'pubyr'} < $maxyr )	{
		$maxyr = $t->{pubyr};
		$focal_taxon->{class} = $t->{'taxon_name'};
		$focal_taxon->{class_no} = $t_no;
	    }
	}
    }
    
    return ($focal_taxon->{class}, $focal_taxon->{order}, $focal_taxon->{family});
}


# The following routines are used by the methods defined above to establish
# the proper query fields and parameters.
#
# =========================================================================

# generateBaseNos ( base_taxa )
# 
# The paramter 'base_taxa' can be either a single taxon_no value or a Taxon
# object, a comma-separated list, a listref, or a hashref.
# 
# This routine returns either a single taxon_no value or a hash whose keys are
# taxon_no values.

sub generateBaseNos {
    
    my ($self, $base_taxa) = @_;
    
    my (%base_nos);
    
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
		
		if ( defined $t->{taxon_no} )
		{
		    $base_nos{$t->{taxon_no}} = 1;
		}
		
		elsif ( defined $t->{orig_no} )
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
	    return {};
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
	
	my $base_no = $base_taxa->{taxon_no} if defined $base_taxa->{taxon_no};
	$base_no = $base_taxa->{orig_no} if defined $base_taxa->{orig_no}
	    and not $base_no > 0;
	
	$base_nos{$base_no} = 1 if $base_no > 0;
    }
    
    elsif ( defined $base_taxa && $base_taxa =~ /^[0-9]+$/ )
    {
	$base_nos{$base_taxa} = 1;
    }
    
    else
    {
	carp "base taxon is undefined or zero";
	return {};
    }
    
    return \%base_nos;
}
    

# generateQueryFields ( field_list )
# 
# The parameter 'include_list' can be either an array of strings or a
# comma-separated concatenation of strings.  Possible values are indicated
# above, near the field list variables.
# 
# This routine returns a field string and a hash which lists extra tables to
# be joined in the query.

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
	
	elsif ( $inc eq 'extant' )
	{
	    $fields .= $EXTANT_FIELDS;
	    $tables{v} = 1;
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
	
	elsif ( $inc eq 'discussion' )
	{
	    $fields .= $DISCUSSION_FIELDS;
	    $tables{ppd} = 1;
	}
	
	elsif ( $inc eq 'specimen' )
	{
	    $fields .= $SPECIMEN_FIELDS;
	}
	
	elsif ( $inc eq 'preservation' )
	{
	    $fields .= $PRESERVATION_FIELDS;
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
	
	elsif ( $inc eq 'img' )
	{
	    $fields .= $IMG_FIELDS;
	    $tables{v} = 1;
	}
	
	elsif ( $inc eq 'size' )
	{
	    $fields .= $SIZE_FIELDS;
	    $tables{v} = 1;
	}
	
	elsif ( $inc eq 'app' )
	{
	    $fields .= $APP_FIELDS;
	    $tables{v} = 1;
	}
	
	elsif ( $inc eq 'phylo' )
	{
	    $fields .= $INT_PHYLO_FIELDS;
	    $tables{pi} = 1;
	}
	
	elsif ( $inc eq 'counts' )
	{
	    $fields .= $COUNT_PHYLO_FIELDS;
	    $tables{pc} = 1;
	}
	
	else
	{
	    carp "unrecognized value '$inc' for option 'fields'";
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

    my ($self, $main_table, $tables, $select, $options) = @_;
    
    my $auth_table = $self->{auth_table};
    my $tree_table = $self->{tree_table};
    my $attrs_table = $self->{attrs_table};
    my $image_table = $self->{image_table};
    
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
    $extra_joins .= "LEFT JOIN $attrs_table as v on v.orig_no = $main_table.orig_no\n"
	if $tables->{v};
    $extra_joins .= "LEFT JOIN interval_map as fei2 on fei2.older_seq = v.first_early_int_seq\n"
	if $tables->{fei};
    $extra_joins .= "LEFT JOIN interval_map as fei on fei.interval_no = fei2.early_st_no\n"
	if $tables->{fei};
    $extra_joins .= "LEFT JOIN interval_map as fli2 on fli2.older_seq = v.first_late_int_seq\n"
	if $tables->{fli};
    $extra_joins .= "LEFT JOIN interval_map as fli on fli.interval_no = fli2.late_st_no\n"
	if $tables->{fli};
    $extra_joins .= "LEFT JOIN interval_map as lei2 on lei2.younger_seq = v.last_early_int_seq\n"
	if $tables->{lei};
    $extra_joins .= "LEFT JOIN interval_map as lei on lei.interval_no = lei2.early_st_no\n"
	if $tables->{lei};
    $extra_joins .= "LEFT JOIN interval_map as lli2 on lli2.younger_seq = v.last_late_int_seq\n"
	if $tables->{fli};
    $extra_joins .= "LEFT JOIN interval_map as lli on lli.interval_no = lli2.late_st_no\n"
	if $tables->{fli};
    $extra_joins .= "LEFT JOIN taxon_ints as pi on pi.ints_no = t.ints_no\n"
	if $tables->{pi};
    $extra_joins .= "LEFT JOIN taxon_counts as pc on pc.orig_no = t.orig_no\n"
	if $tables->{pc};
#    $extra_joins .= "LEFT JOIN (SELECT orig_no as image_no FROM $image_table as ti WHERE ti.orig_no = t.orig_no and priority >= 0 ORDER BY priority desc LIMIT 1) as ti\n"
#	if $tables->{ti};
    
    if ( $tables->{pa} and $main_table !~ /^o/ )
    {
	if ( $options->{senior} )
	{
	    $extra_joins .= "LEFT JOIN $tree_table as pt2 on pt2.orig_no = t.immpar_no\n";
	    $extra_joins .= "LEFT JOIN $tree_table as pt on pt.orig_no = pt2.synonym_no\n";
	}
	
	else
	{
	    $extra_joins .= "LEFT JOIN $tree_table as pt on pt.orig_no = t.immpar_no\n";
	}
	
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
    
    $extra_joins .= "
		LEFT JOIN person as ppd on ppd.person_no = $main_table.discussed_by\n"
	if $tables->{ppd};
    
    # Return results
    
    return $extra_joins;
}


# generateCountLimitExpr ( options )
# 
# Generate the SQL query modifiers indicated by the specified options

sub generateCountLimitExpr {
    
    my ($self, $options) = @_;
    
    my $count_expr = $options->{count} ? 'SQL_CALC_FOUND_ROWS' : '';
    
    my $limit_expr = '';
    
    my $limit = $options->{limit};
    my $offset = $options->{offset};
    
    if ( defined $offset and $offset > 0 )
    {
	$offset += 0;
	$limit = $limit eq 'all' ? 10000000 : $limit + 0;
	return ($count_expr, "LIMIT $offset,$limit");
    }
    
    elsif ( defined $limit and $limit ne 'all' )
    {
	return ($count_expr, "LIMIT " . ($limit + 0));
    }
    
    else
    {
	return ($count_expr, '');
    }
}

# generateExcludeFilter ( table, exclusion )
# 
# The parameter 'exclusion' can be either an object of type Taxon::Exclude
# generated by the method C<generateExclusion>, or it can be a single taxon_no
# value or Taxon object or a listref or hashref of them.  We return a filter
# clause which will exclude the specified taxa and their children from a query.

sub generateExcludeFilter {

    my ($self, $table, $exclusion) = @_;
    
    # Unless $exclusion is an object of type Taxon::Exclude, make it into one.
    
    return unless defined $exclusion;
    
    unless ( ref $exclusion eq 'Taxon::Exclude' )
    {
	$exclusion = $self->generateExclusion($exclusion);
    }
    
    return $exclusion->{filter_string};
}


# generateStatusFilter ( table, status )
# 
# The parameter 'status' must be one of 'valid', 'senior', 'invalid' or
# 'all'. We return a list of filter clauses which will select taxa of the
# specified status.

sub generateStatusFilter {

    my ($self, $table, $status) = @_;
    
    # If the parameter is undefined, we return nothing (so the query goes
    # ahead with no status filtering, equivalent to 'all').
    
    return unless defined $status and $status ne 'all';
    
    # Return the indicated filter clause:
    
    if ( $status eq 'valid' )
    {
	return "$table.status in ('belongs to', 'subjective synonym of', 'objective synonym of')";
    }
    
    elsif ( $status eq 'senior' )
    {
	return "$table.status = 'belongs to'";
    }
    
    elsif ( $status eq 'invalid' )
    {
	return "$table.status not in ('belongs to', 'subjective synonym of', 'objective synonym of')";
    }
    
    # If an invalid value was given, return a clause which will select
    # nothing.  This is probably better than selecting everything.
    
    else
    {
	carp "invalid value for option 'status': '$status'";
	return "$table.status = 'SELECT NOTHING'";
    }
}


# generateRankFilter ( table, rank_list )
# 
# The parameter 'rank_list' can be either an array of strings or a
# comma-separated concatenation of strings.  Each string can be a single
# taxonomic rank ('family', 'genus') and may be prefixed by a comparison
# operator ('>', '<', '>=', '<=').  All such prefixed clauses are and'ed
# together as one clause, and then or'ed together with all of the individual
# ones.

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
    
    my (%include_ranks, @op_clauses, $op_clause, $include_clause);
    
    foreach my $rank (@$rank_list)
    {
	$rank = lc $rank;
	
	if ( $rank =~ /^(>=?|<=?)?\s*(.*)/ )
	{
	    carp "invalid taxonomic rank '$2'" unless $TAXONOMIC_RANK{$2};
	    
	    if ( $1 )
	    {
		push @op_clauses, "$table.taxon_rank $1 $TAXONOMIC_RANK{$2}";
	    }
	    
	    else
	    {
		$include_ranks{$TAXONOMIC_RANK{$2}} = 1;
	    }
	}
	
	else
	{
	    carp "invalid taxonomic rank '$rank'";
	}
    }
    
    # Merge together both types of clauses, those with operators and those without.
    
    if ( @op_clauses )
    {
	$op_clause = join(' and ', @op_clauses);
    }
    
    if ( %include_ranks )
    {
	$include_clause = "$table.taxon_rank in (" . join(",", keys %include_ranks) . ")";
    }
    
    # If both kinds of clauses were found, return a disjunction.  Otherwise
    # return whichever was found.
    
    if ( $op_clause && $include_clause )
    {
	return "(($op_clause) or $include_clause)";
    }
    
    elsif ( $op_clause )
    {
	return $op_clause;
    }
    
    elsif ( $include_clause )
    {
	return $include_clause;
    }
    
    # If no valid ranks were specified, then return a clause which will select
    # nothing.  This is better than selecting everything.
    
    else
    {
	return "$table.taxon_rank = 0";
    }
}


# generateKingdomFilter ( table, kingdom )
# 
# The parameter 'kingdom' must be the name of a kingdom of life or a
# nomenclatural code.  We return a list of filter clauses which will select
# the specified kingdom or kingdoms.

sub generateKingdomFilter {
    
    my ($self, $table, $kingdom) = @_;
    
    # If the parameter is undefined, we return nothing (so the query goes
    # ahead with no kingdom filtering).
    
    return unless defined $kingdom;
    
    $kingdom = lc $kingdom;
    
    # Return the appropriate filter expression.
    
    if ( $KINGDOM_ALIAS{$kingdom} )
    {
	return "$table.kingdom = '$KINGDOM_ALIAS{$kingdom}'";
    }
    
    elsif ( $kingdom eq 'icn' )
    {
	return "$table.kingdom in ('Plantae', 'Fungi', 'Eukaryota')";
    }
    
    # If no valid kingdom was given, return a filter expression that will
    # select nothing.
    
    else
    {
	carp "invalid kingdom '$kingdom'";
	return "table.kingdom = 'SELECT_NOTHING'";
    }
}


our (%BOOLEAN_VALUE) = (1 => 1, 'yes' => 1, 'no' => 1, 0 => 0, 'no' => 0, 'false' => 0);

# generateExtantFilter ( table, kingdom )
# 
# The parameter 'extant' specifies that we are looking for taxa which are or
# are not extant.  We return a list of filter clauses which will select the
# specified kingdom or kingdoms.

sub generateExtantFilter {
    
    my ($self, $table, $extant) = @_;
    
    # If the parameter is undefined or unrecognized, we return nothing (so the
    # query goes ahead with no filtering).
    
    return unless defined $extant;
    
    $extant = lc $extant;
    
    if ( $extant eq 'unknown' )
    {
	return "$table.is_extant is null";
    }
    
    elsif ( exists $BOOLEAN_VALUE{$extant} )
    {
	return "$table.is_extant = $BOOLEAN_VALUE{$extant}";
    }
    
    else
    {
	carp "invalid value '$extant' for option 'extant'";
	return;
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


# generateAttributeFilter ( attribute, value_or_values )
# 
# Return a filter clause which will select only taxa for which the specified
# attribute matches one or more of the specified values.

sub generateAttributeFilter {
    
    my ($self, $table, $option_name, $attribute, $value_list) = @_;
    
    # If the parameter is undefined, we return nothing (so the query goes
    # ahead with no author filtering).
    
    return unless defined $value_list;
    
    # Next, we turn $value_list into a reference to an actual list unless it
    # already is one.
    
    unless ( ref $value_list )
    {
	my @strings = split /\s*,\s*/, $value_list;
	$value_list = \@strings;
    }
    
    elsif ( ref $value_list ne 'ARRAY' )
    {
	croak "option '$option_name' must be either a string or an arrayref";
    }
    
    # Now, generate the filter.  If no valid values are found, generate an
    # expression that will guarantee an empty result set.
    
    my %include_values;
    
    foreach my $value (@$value_list)
    {
	$value =~ s/\s+$//;
	
	if ( $value ne '' )
	{
	    my $dbh = $self->{dbh};
	    my $clean_string = $dbh->quote($value);
	    $include_values{$clean_string} = 1;
	}
    }

    if ( keys %include_values )
    {
	my $expr = "(" . join(",", keys %include_values) . ")";
	
	return "$table.$attribute in $expr";
    }
    
    else
    {
        return "$table.$attribute = 'SELECT NOTHING'";
    }
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


# computeAncestry ( base_nos, common )
# 
# Use the ancestry scratch table to compute the set of common parents of the
# specified taxa (keys of the base_nos hash).  If the parameter 'common' is
# true, then stop when it is clear that we've passed the nearest common
# ancestor of all the base taxa.
# 
# This function is only necessary because MySQL stored procedures cannot work
# on temporary tables.  :(

sub computeAncestry {

    my ($self, $base_nos) = @_;
    
    my $dbh = $self->{dbh};
    my $auth_table = $self->{auth_table};
    my $tree_table = $self->{tree_table};
    my $scratch_table = $self->{scratch_table};
    
    my $result;
    
    # Create a temporary table by which we can extract information from
    # $scratch_table and convey it past the table locks.
    
    $result = $dbh->do("DROP TABLE IF EXISTS ancestry_temp");
    $result = $dbh->do("CREATE TEMPORARY TABLE ancestry_temp (
				orig_no int unsigned primary key,
				is_base tinyint unsigned) Engine=MyISAM");
    
    # Lock the tables that will be used by the stored procedure
    # "compute_ancestry".
    
    $result = $dbh->do("LOCK TABLES $scratch_table WRITE,
				    $scratch_table as s WRITE,
				    $auth_table READ,
				    $tree_table READ,
				    ancestry_temp WRITE");
    
    # We need a try block to make sure that the table locks are released
    # no matter what else happens.
    
    try
    {
	my $taxon_nos = join(',', keys %$base_nos);
	$result = $dbh->do("CALL compute_ancestry('$auth_table','$tree_table', '$taxon_nos')");
	
	# Finally, copy the information out of $scratch_table to a
	# temporary table so that we can release the locks.
	
	$result = $dbh->do("INSERT INTO ancestry_temp SELECT * FROM $scratch_table"); 
    }
    
    finally {
	$dbh->do("UNLOCK TABLES");
	die $_[0] if defined $_[0];
    };
    
    # There is no need to return anything, since the results of this function
    # are in the rows of the 'ancestry_temp' table.  But we can stop here on
    # debugging.
    
    my $a = 1;
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

sub getSelf {
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

