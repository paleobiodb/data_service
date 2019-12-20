# 
# The Paleobiology Database
# 
#   Taxonomy.pm
# 
# This module provides an interface between the rest of the paleobiodb and the taxonomy tables.
# It is designed for use by the data service, but will in the future be extended so that the
# classic code can be rewritten to use it too.


package Taxonomy;

use TaxonDefs qw(%TAXON_TABLE %TAXON_RANK %RANK_STRING);
use TableDefs qw($INTERVAL_MAP $OCC_MATRIX $SPEC_MATRIX $COLL_MATRIX %IDP);
use Carp qw(carp croak);
use Try::Tiny;

use strict;
use feature 'unicode_strings';


our (%FIELD_LIST, %FIELD_TABLES);


=head1 NAME

Taxonomy.pm

=head1 DESCRIPTION

This module provides  an interface to the paleobiodb  taxonomy tables.  It is designed  for use by
the data service, but will in the future be  extended so that the classic code can be rewritten to
use it too.

=head1 SYNOPSIS

    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    
    my @base_taxa = $taxonomy->resolve_names($taxon_name);
    my @taxa = $taxonomy->list_taxa('all_children', \@base_taxa, { status => 'valid' });

=head1 METHODS

=head3 new ( dbh, tree_table_name )

    $taxonomy = Taxonomy->new($dbh, 'taxon_trees');

Creates a new Taxonomy object, which will use the database connection given by C<$dbh> and the
taxonomy table named by C<$tree_table_name>.  As noted above, the main taxonomy table is currently
called I<taxon_trees>.  For now this is the only one defined, but others may be defined at some
point.  The new definitions will need to be put into C<TaxonDefs.pm>.

=cut

sub new {

    my ($class, $dbh, $table_name) = @_;
    
    my $t = $TAXON_TABLE{$table_name};
    
    croak "unknown tree table '$table_name'" unless ref $t;
    croak "bad database handle" unless ref $dbh;
    
    my $self = { dbh => $dbh, 
		 sql_string => '',
		 TREE_TABLE => $table_name,
		 SEARCH_TABLE => $t->{search},
	         ATTRS_TABLE => $t->{attrs},
		 AGES_TABLE => $t->{ages},
		 ECOTAPH_TABLE => $t->{ecotaph},
		 ETBASIS_TABLE => $t->{etbasis},
		 INTS_TABLE => $t->{ints},
		 LOWER_TABLE => $t->{lower},
		 COUNTS_TABLE => $t->{counts},
		 AUTH_TABLE => $t->{authorities},
		 OP_TABLE => $t->{opinions},
		 OP_CACHE => $t->{opcache},
		 REFS_TABLE => $t->{refs},
		 NAMES_TABLE => $t->{names},
		 SCRATCH_TABLE => 'ancestry_scratch',
	       };
        
    bless $self, $class;
    
    return $self;
}


# The following hashes list the various options accepted by the methods below.

my (%STD_OPTION) = ( fields => 1, 
		     order => 1,
		     record_type => 1,
		     debug_out => 1,
		     table => 1,
		     count => 1,
		     limit => 1,
		     offset => 1,
		     return => 1 );

my (%TAXON_OPTION) = ( rank => 1,
		       min_rank => 1,
		       max_rank => 1,
		       min_ma => 1, 
		       max_ma => 1,
		       exact => 1,
		       current => 1,
		       extant => 1, 
		       depth => 1,
		       status => 1, 
		       pres => 1,
		       higher => 1,
		       reference_no => 1,
		       immediate => 1,
		       all_variants => 1,
		       current_only => 1,
		       occ_name => 1,
		       exclude => 1,
		       min_pubyr => 1,
		       max_pubyr => 1,
		       min_modyr => 1, 
		       max_modyr => 1,
		       reference_no => 1,
		       created_after => 1,
		       created_before => 1,
		       modified_after =>1,
		       modified_before => 1,
		       authorized_by => 1,
		       entered_by => 1,
		       modified_by => 1,
		       authent_by => 1,
		       touched_by => 1, );

my (%RESOLVE_OPTION) = ( all_names => 1,
			 common => 1 );

my (%ASSOC_OPTION) = ( reference_no => 1,
		       opinion_no => 1,
		       ident_select => 1,
		       ident_qual => 1,
		       extra_filters => 1,
		       record_order => 1 );

my (%OP_OPTION) = ( op_type => 1,
		    op_author => 1,
		    op_min_pubyr => 1,
		    op_max_pubyr => 1,
		    op_created_after => 1,
		    op_created_before => 1,
		    op_modified_after =>1,
		    op_modified_before => 1,
		    op_authorized_by => 1,
		    op_entered_by => 1,
		    op_modified_by => 1,
		    op_authent_by => 1,
		    op_touched_by => 1 );

my (%REF_OPTION) = ( ref_type => 1, 
		     ref_author => 1,
		     ref_min_pubyr => 1,
		     ref_max_pubyr => 1,
		     ref_title => 1,
		     ref_pubtitle => 1,
		     ref_language => 1,
		     ref_created_after => 1,
		     ref_created_before => 1,
		     ref_modified_after =>1,
		     ref_modified_before => 1,
		     ref_authorized_by => 1,
		     ref_entered_by => 1,
		     ref_modified_by => 1,
		     ref_authent_by => 1,
		     ref_touched_by => 1 );

my (%COMMON_OPTION) = ( created_after => 1,
			created_before => 1,
			modified_after =>1,
			modified_before => 1,
			authorized_by => 1,
			entered_by => 1,
			modified_by => 1,
			authent_by => 1,
			touched_by => 1,
			op_created_after => 1,
			op_created_before => 1,
			op_modified_after =>1,
			op_modified_before => 1,
			op_authorized_by => 1,
			op_entered_by => 1,
			op_modified_by => 1,
			op_authent_by => 1,
			op_touched_by => 1,
			ref_created_after => 1,
			ref_created_before => 1,
			ref_modified_after =>1,
			ref_modified_before => 1,
			ref_authorized_by => 1,
			ref_entered_by => 1,
			ref_modified_by => 1,
			ref_authent_by => 1,
			ref_touched_by => 1 );

my (%REF_SELECT_VALUE) = ( all => 1,
			   auth => 1,
			   var => 1,
			   class => 1,
			   taxonomy => 1,
			   ops => 1,
			   specs => 1,
			   occs => 1,
			   colls => 1 );

my (%OP_SELECT_VALUE) = ( all => 1,
			  class => 1,
			  taxonomy => 1,
			  valid => 1,
			  accepted => 1,
			  senior => 1,
			  junior => 1,
			  invalid => 1 );

my (%RECORD_TYPE_VALUE) = ( opinions => 1,
			    refs => 1,
			    taxa => 1 );

my (%RECORD_BLESS) = ( taxa => 'PBDB::Taxon',
		       refs => 'PBDB::Reference',
		       opinions => 'PBDB::Opinion' );

my $VALID_TAXON_ID = qr{ ^ (?: $IDP{TXN} | $IDP{VAR} )? ( [0-9]+ ) $ }xsi;
my $VALID_OPINION_ID = qr{ ^ (?: $IDP{OPN} )? ( [0-9]+ ) $ }xsi;
my $VALID_REF_ID = qr{ ^ (?: $IDP{REF} )? ( [0-9]+ ) $ }xsi;
my $VALID_PERSON_ID = qr{ ^ (?: $IDP{PER} )? ( [0-9]+ ) $ }xsi;
my $NUMERIC_ID = qr{ ^ -? \d+ $ }xs;

# Type codes for opinions and references

my $TYPE_AUTH = "'A'";
my $TYPE_VAR = "'V'";
my $TYPE_CLASS = "'C'";
my $TYPE_UNSEL = "'U'";
my $TYPE_OCC = "'O'";
my $TYPE_SPEC = "'S'";
my $TYPE_PRIMARY = "'P'";
my $TYPE_SUPPRESSED = "'X'";

=head3 last_sql

Return the SQL statement used by the last query method called on this Taxonomy object.  This means
the last SQL statement used to actually fetch taxon records, ignoring any auxiliary requests.
This info should be used only for debugging purposes.  Returns the empty string if no such SQL
statement has been generated.

=cut

sub last_sql {
    
    my ($taxonomy) = @_;
    
    return $taxonomy->{sql_string} || '';
}


=head3 last_rowcount

Returns the rowcount of the result generated by the last query method called this Taxonomy object.
This information is only available if the option 'count' was passed to that method.  Returns the
undefined value if no results have been generated, or if 'count' was not specified.

=cut

sub last_rowcount {
    
    my ($taxonomy) = @_;
    
    return $taxonomy->{sql_rowcount};
}


=head3 clear_sql

Clears the information used by L</last_sql> and L</last_rowcount>.  This method is automatically
called at the beginning of each query method.

=cut

sub clear_sql {
    
    my ($taxonomy) = @_;
    
    $taxonomy->{sql_string} = '';
    delete $taxonomy->{sql_rowcount};
}


=head3 list_warnings

Returns a list of warning messages (if any) that were generated by the most recent query method
called on this Taxonomy object.

=cut

sub list_warnings {
    
    my ($taxonomy) = @_;
    return unless $taxonomy->{warnings};
    return @{$taxonomy->{warnings}};
}


=head3 has_warning ( code )

Returns true if the most recent query method called on this Taxonomy object
generated a warning with the specified code, false otherwise.

=cut

sub has_warning {
    
    my ($taxonomy, $code) = @_;
    
    return 1 if $taxonomy->{warning_codes}{$code};	# true if code was flagged
    return;						# false otherwise
}


=head3 add_warning ( code, message... )

Adds a warning message to the list that will be returned by L</list_warnings>.  This method is
called internally by the query methods.  The value of C<$code> is ignored for now.

=cut

sub add_warning {

    my ($taxonomy, $code, @messages) = @_;
    
    foreach my $m (@messages)
    {
	push @{$taxonomy->{warnings}}, $m;
	push @{$taxonomy->{warning_codes}{$code}}, $m;
    }
}


=head3 clear_warnings

Clears any warning messages associated with this Taxonomy object.  This method is called
automatically at the beginning of each query method.

=cut

sub clear_warnings {

    my ($taxonomy) = @_;
    delete $taxonomy->{warnings};
    delete $taxonomy->{warning_codes};
}


=head2 QUERY ARGUMENTS AND OPTIONS



=head2 QUERY METHODS

=head3 list_taxa_simple ( base_nos, options )

This method returns a set of Taxon objects representing the taxa specified by $base_nos.  It takes
all of the Standard options, except that 'return' is ignored unless its value is
either 'list' or 'listref'.

It also accepts all of the Taxon options, but those that are only relevant to subtrees are ignored.

The basic purpose of this routine is to generate Taxon objects from identifiers, or to generate
Taxon objects that contain additional information from an existing list of Taxon objects.

=cut

sub list_taxa_simple {

    my ($taxonomy, $base_nos, $options) = @_;
    
    # First check the arguments.
    
    $taxonomy->clear_warnings;
    $taxonomy->clear_sql;
    
    croak "list_taxa_simple: second argument must be a hashref if given"
	if defined $options && ref $options ne 'HASH';
    $options ||= {};
    
    my $return_type = lc $options->{return} || 'list';
    my $base_string;
    
    unless ( $base_string = $taxonomy->generate_id_string($base_nos, 'ignore_exclude', $options->{exact}) )
    {
	return $return_type eq 'listref' ? [] : ();
    }
    
    foreach my $key ( keys %$options )
    {
	croak "list_taxa_simple: invalid option '$key'\n"
	    unless $STD_OPTION{$key} || $TAXON_OPTION{$key};
    }
    
    # Then generate an SQL statement according to the specified base_no and options.
    
    my ($sql, $result_list);
    
    my $tables = { };
    my $auth_expr;
    
    if ( $options->{exact} )
    {
	$tables->{use_a} = 1;
	$auth_expr = "a.taxon_no = base.taxon_no";
    }
    
    else
    {
	$auth_expr = "a.taxon_no = t.spelling_no";
    }
    
    my $fieldspec = $options->{fields} || 'SIMPLE';
    my @fields = $taxonomy->generate_fields($fieldspec, $tables);
    
    push @fields, "base.taxon_no as base_no" unless $options->{exact};
    
    my $fields = join ', ', @fields;
    
    my @filters = "base.taxon_no in ($base_string)";
    push @filters, $taxonomy->taxon_filters($options, $tables);
    push @filters, $taxonomy->refno_filter($options, 'a');
    my $filters = join( q{ and }, @filters);
    
    my $other_joins = $taxonomy->taxon_joins('t', $tables);
    $other_joins .= $taxonomy->ref_joins('a', $tables);
    
    my $auth_table = $taxonomy->{AUTH_TABLE};
    my $tree_table = $taxonomy->{TREE_TABLE};
    
    $sql = "
	SELECT $fields
	FROM $auth_table as base JOIN $tree_table as t using (orig_no)
		JOIN $auth_table as a on $auth_expr
		$other_joins
	WHERE $filters
	ORDER BY t.lft\n";
    
    # Save this statement and execute it.
    
    $taxonomy->{sql_string} = $sql;
    
    if ( ref $options->{debug_out} eq 'CODE' )
    {
	&{$options->{debug_out}}($sql);
    }
    
    try {
	$result_list = $taxonomy->{dbh}->selectall_arrayref($sql, { Slice => {} });
    }
    
    catch {
	die $_ if $_;
    };
    
    # If we got some results, then process the list and return it.
    
    if ( ref $result_list eq 'ARRAY' )
    {
	# Bless each result object into class 'PBDB::Taxon'.
	
	bless($_, 'PBDB::Taxon') foreach @$result_list;
	
	# Also set the exclusion flags properly.
	
	if ( $options->{exclude} )
	{
	    map { $_->{exclude} = 1 } @$result_list;
	}
	
	else
	{
	    $taxonomy->copy_exclusions($result_list, $base_nos);
	}
	
	# Set the rowcount, and return the results in the format requested.
	
	$taxonomy->{sql_rowcount} = scalar(@$result_list);
	return $result_list if $return_type eq 'listref';
	return @$result_list;
    }
    
    # Otherwise, we return an empty list or listref.
    
    else
    {
	$taxonomy->{sql_rowcount} = 0;
	return [] if $return_type eq 'listref';
	return;
    }
}


=head3 list_subtree ( base_nos, options )

This method returns a set of Taxon objects corresponding to the subtree rooted at $base_nos.  It
takes all of the Standard options, except that 'return' is ignored unless its value is either
'list' or 'listref'.

It also accepts all of the Taxon options.

This is intended to be a simple routine to cover a common case.  For more flexibility, you can use
L</list_taxa>.

=cut

sub list_subtree {

    my ($taxonomy, $base_nos, $options) = @_;
    
    # First check the arguments.
    
    $taxonomy->clear_warnings;
    $taxonomy->clear_sql;
    
    croak "list_subtree: second argument must be a hashref if given"
	if defined $options && ref $options ne 'HASH';
    $options ||= {};
    
    my $return_type = lc $options->{return} || 'list';
    my $base_string;
    
    unless ( $base_string = $taxonomy->generate_id_string($base_nos, 0, $options->{exact} ) )
    {
	return $return_type eq 'listref' ? [] : ();
    }
    
    foreach my $key ( keys %$options )
    {
	croak "list_subtree: invalid option '$key'\n"
	    unless $STD_OPTION{$key} || $TAXON_OPTION{$key};
    }
    
    # Then generate an SQL statement according to the specified base_no and options.
    
    my $tables = {};
    my $fieldspec = $options->{fields} || 'SIMPLE';
    my @fields = $taxonomy->generate_fields($fieldspec, $tables);
    my $fields = join ', ', @fields;
    
    my @filters = "base.taxon_no in ($base_string)";
    push @filters, $taxonomy->taxon_filters($options, $tables);
    push @filters, $taxonomy->exclusion_filters($base_nos);
    push @filters, $taxonomy->refno_filter($options, 'a');
    my $filters = @filters ? join ' and ', @filters : '1=1';
    
    my $other_joins = $taxonomy->taxon_joins('t', $tables);
    $other_joins .= $taxonomy->ref_joins('a', $tables);
    
    my $auth_table = $taxonomy->{AUTH_TABLE};
    my $tree_table = $taxonomy->{TREE_TABLE};
    
    my $sql =  "SELECT $fields
		FROM $auth_table as base JOIN $tree_table as tb using (orig_no)
			JOIN $tree_table as t on t.lft between tb.lft and tb.rgt
			JOIN $auth_table as a on a.taxon_no = t.spelling_no
			$other_joins
		WHERE $filters
		ORDER BY t.lft\n";
    
    $taxonomy->{sql_string} = $sql;
    
    if ( ref $options->{debug_out} eq 'CODE' )
    {
	&{$options->{debug_out}}($sql);
    }
    
    my $result_list;
    
    try {
	$result_list = $taxonomy->{dbh}->selectall_arrayref($sql, { Slice => {} });
    }
    
    catch {
	die $_ if $_;
    };
    
    # If we got some results, then bless each object into class 'PBDB::Taxon',
    # set the result count, and return the list or listref.
    
    if ( ref $result_list eq 'ARRAY' )
    {
	bless($_, 'PBDB::Taxon') foreach @$result_list;
	
	$taxonomy->{sql_rowcount} = scalar(@$result_list);
	
	return $result_list if $return_type eq 'listref';
	return @$result_list;
    }
    
    else
    {
	$taxonomy->{sql_rowcount} = 0;
	return [] if $return_type eq 'listref';
	return;
    }
}


=head3 list_taxa ( rel, base_nos, options )

This is the most flexible of all the taxon query routines.  It takes all of the Standard options,
and all of the Taxon options.  Its basic operation is to list all taxa related to the ones
specified in $base_nos, using the relationship $rel.

The available relationships are as follows:

=over

=item current

List the most current variant of each specified taxon (essentially the same as list_taxa_simple)
  
=item exact

List the exactly specified variant of each taxon (esentially the same as list_taxa_simple with the
'exact' option) 

=item variants

List all variants of each specified taxon

=item senior

List the senior synonym of each specified taxon

=item synonyms

List all synonyms of each specified taxon

=item accepted

List the currently accepted taxon corresponding to each specified taxon

=item senpar

List the parent taxon of each specified taxon.  This is equivalent to the
senior synonym of the immediate parent taxon.

=item immpar

List the immediate parent of each specified taxon.

=item children

List the taxa immediately contained within each specified taxon and all its synonyms.  If the
option 'immediate' is also specified, then list only immediate children of the specified taxa,
and not children of their synonyms.

If some of the base taxa are marked as excluded, then children of those taxa will not be returned.

=item all_children

List all taxa contained within the any of the specified taxa or their synonyms.  If the option
'immediate' is also specified, then list only children of the specified taxa, and not children
of their synonyms.

If some of the base taxa are marked as excluded, then children of those taxa will not be returned.

=item all_parents

List all taxa that contain any of the specified taxa.

=item common

List the most specified taxon that contains all of the specified taxa.

=item all_taxa

List all taxa in the database, current variants only.

=item all_records

List all taxonomic names in the database, including all variants.

=back

=cut

sub list_taxa {
    
    my ($taxonomy, $rel, $base_nos, $options) = @_;
    
    # First check the arguments.
    
    $taxonomy->clear_warnings;
    $taxonomy->clear_sql;
    
    croak "list_taxa: third argument must be a hashref if given"
	if defined $options && ref $options ne 'HASH';
    $options ||= {};
    
    my $return_type = lc $options->{return} || 'list';
    my $base_string;
    
    croak "list_taxa: second argument must be a valid relationship\n"
	unless defined $rel;
    
    unless ( $rel eq 'all_taxa' || $rel eq 'all_records' )
    {
	my $ignore_exclude = ($rel =~ /^exact|^current|^accepted|^senior/);
	my $exact = ($rel eq 'exact' || $options->{exact});
	
	$base_string = $taxonomy->generate_id_string($base_nos, $ignore_exclude, $exact);
	
	unless ( $base_string || $rel eq 'occs' || $rel eq 'specs' )
	{
	    return $return_type eq 'listref' ? [] : ();
	}
    }
    
    foreach my $key ( keys %$options )
    {
	croak "list_taxa: invalid option '$key'\n"
	    unless $STD_OPTION{$key} || $TAXON_OPTION{$key};
    }
    
    my $tables = { };
    my $group_expr = 't.orig_no';
    my $auth_expr = 'a.taxon_no = t.spelling_no';
    
    $rel = 'current' if $rel eq 'self';
    $rel = 'all_taxa' if $rel eq 'all_records' && $options->{current_only};
    
    if ( $options->{all_variants} || $rel eq 'variants' || $rel eq 'all_records' )
    {
	$tables->{use_a} = 1;
	$rel = 'variants' if $rel eq 'exact' || $rel eq 'current';
	$group_expr = 'a.taxon_no';
	$auth_expr = 'a.orig_no = t.orig_no';
    }
    
    elsif ( $rel eq 'exact' )
    {
	$tables->{use_a} = 1;
	$auth_expr = 'a.taxon_no = base.taxon_no';
    }
    
    my $fieldspec = $options->{fields} || 'SIMPLE';
    $fieldspec = 'ID' if $return_type eq 'id';
    my @fields = $taxonomy->generate_fields($fieldspec, $tables);
    
    my $count_expr = $options->{count} ? 'SQL_CALC_FOUND_ROWS' : '';
    my $order_expr = $taxonomy->taxon_order($rel, 'taxa', $options, $tables);
    my $limit_expr = $taxonomy->simple_limit($options);
    
    my $auth_table = $taxonomy->{AUTH_TABLE};
    my $tree_table = $taxonomy->{TREE_TABLE};
    
    my $copy_exclusions;
    
    my ($sql, @sql_strings, $drop_table);
    
    if ( $rel eq 'exact' || $rel eq 'current' || $rel eq 'variants' )
    {
	$copy_exclusions = 1;
	
	push @fields, 'base.taxon_no as base_no' if $rel ne 'exact';
	push @fields, 'if(a.taxon_no = t.spelling_no, 1, 0) as is_current' if $rel eq 'variants';
	my $fields = join ', ', @fields;
	
	my @filters = "base.taxon_no in ($base_string)";
	push @filters, $taxonomy->taxon_filters($options, $tables);
	push @filters, $taxonomy->refno_filter($options, 'a');
	my $filters = join( q{ and }, @filters);
	
	my $other_joins = $taxonomy->taxon_joins('t', $tables);
	$other_joins .= $taxonomy->ref_joins('a', $tables);
	
	$order_expr ||= 'ORDER BY base_no, is_current desc' if $rel eq 'variants';
	$order_expr ||= 'ORDER BY NULL';
	
	$sql = "SELECT $count_expr $fields
		FROM $auth_table as base JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on $auth_expr
			$other_joins
		WHERE $filters
		GROUP BY a.taxon_no $order_expr $limit_expr\n";
    }
    
    elsif ( $rel eq 'accepted' || $rel eq 'senior' || $rel eq 'parent' || $rel eq 'senpar' || $rel eq 'immpar' )
    {
	$copy_exclusions = 1 if $rel eq 'accepted' || $rel eq 'senior';
	
	push @fields, 'base.orig_no as base_no';
	my $fields = join ', ', @fields;
	
	if ( $rel eq 'parent' )
	{
	    $rel = $options->{immediate} ? 'immpar' : 'senpar';
	}
	
	my $rel_field = $rel eq 'senior' ? 'synonym_no' : $rel . '_no';
	
	my @filters = "base.taxon_no in ($base_string)";
	push @filters, $taxonomy->taxon_filters($options, $tables);
	push @filters, $taxonomy->refno_filter($options, 'a');
	my $filters = join( q{ and }, @filters);
	
	my $other_joins = $taxonomy->taxon_joins('t', $tables);
	$other_joins .= $taxonomy->ref_joins('a', $tables);
	
	$order_expr ||= 'ORDER BY base_no';
	
	$sql = "SELECT $count_expr $fields
		FROM $auth_table as base JOIN $tree_table as tb using (orig_no)
			JOIN $tree_table as t on t.orig_no = tb.$rel_field
			JOIN $auth_table as a on $auth_expr
			$other_joins
		WHERE $filters
		GROUP BY $group_expr $order_expr $limit_expr\n";
    }
    
    elsif ( $rel eq 'synonyms' || $rel eq 'juniors' || $rel eq 'seniors' || $rel eq 'children' )
    {
	push @fields, 'base.orig_no as base_no';
	push @fields, 'if(t.orig_no = t.synonym_no, 1, 0) as is_senior';
	my $fields = join ', ', @fields;
	
	my ($sel_field, $rel_field);
	
	# Select the fields on which to query
	
	if ( $rel eq 'synonyms' || $rel eq 'juniors' || $rel eq 'seniors' )
	{
	    $rel_field = 'synonym_no';
	    $sel_field = 'synonym_no';
	}
	
	elsif ( $options->{immediate} )
	{
	    $rel_field = 'immpar_no';
	    $sel_field = 'orig_no';
	}
	
	else
	{
	    $rel_field = 'senpar_no';
	    $sel_field = 'synonym_no';
	}
	
	my @filters = "base.taxon_no in ($base_string)";
	push @filters, $taxonomy->taxon_filters($options, $tables);
	push @filters, $taxonomy->exclusion_filters($base_nos);
	
	push @filters, "t.lft > tb.lft and t.lft <= tb.rgt" if $rel eq 'juniors';
	push @filters, "t.lft < tb.lft and t.rgt > tb.rgt" if $rel eq 'seniors';
	
	push @filters, $taxonomy->refno_filter($options, 'a');
	
	my $filters = join( q{ and }, @filters);
	
	my $other_joins = $taxonomy->taxon_joins('t', $tables);
	my $ref_joins = $taxonomy->ref_joins('a', $tables);
	
	$order_expr ||= 'ORDER BY base_no, is_senior desc';
	
	$sql = "SELECT $count_expr $fields
		FROM $auth_table as base JOIN $tree_table as tb using (orig_no)
			JOIN $tree_table as t on t.$rel_field = tb.$sel_field
			JOIN $auth_table as a on $auth_expr
			$other_joins
		WHERE $filters
		GROUP BY $group_expr $order_expr $limit_expr\n";
    }
    
    elsif ( $rel eq 'all_children' )
    {
	if ( $options->{immediate} )
	{
	    push @fields, "tb.orig_no as base_no";
	}
	
	else
	{
	    push @fields, "tb2.orig_no as base_no";
	}
	
	# push @fields, 'base.orig_no as base_no';
	#push @fields, 'if (t.orig_no = base.orig_no, 1, 0) as is_base';
	my $fields = join ', ', @fields;
	
	my @filters = "base.taxon_no in ($base_string)";
	push @filters, $taxonomy->taxon_filters($options, $tables);
	push @filters, $taxonomy->exclusion_filters($base_nos);
	push @filters, $taxonomy->refno_filter($options, 'a');
	
	my ($joins);
	
	if ( $options->{immediate} )
	{
	    $joins = "JOIN $tree_table as t on t.lft between tb.lft and tb.rgt";
	    push @filters, $taxonomy->immediate_filters($base_string, $options);
	}
	
	else
	{
	    $joins = "JOIN $tree_table as tb2 on tb2.orig_no = tb.synonym_no
		JOIN $tree_table as t on t.lft between tb2.lft and tb2.rgt";
	}
	
	my $filters = join( q{ and }, @filters);
	
	my $other_joins = $taxonomy->taxon_joins('t', $tables);
	$other_joins .= $taxonomy->ref_joins('a', $tables);
	
	$order_expr ||= 'ORDER BY t.lft';
	
	$sql = "SELECT $count_expr $fields
		FROM $auth_table as base JOIN $tree_table as tb using (orig_no)
			$joins
			JOIN $auth_table as a on $auth_expr
			$other_joins
		WHERE $filters
		GROUP BY $group_expr $order_expr $limit_expr\n";
    }
    
    elsif ( $rel eq 'all_parents' )
    {
	# First select into the temporary table 'ancestry_temp' the set of
	# orig_no values representing the ancestors of the taxa identified by
	# $base_string.
	
	$taxonomy->compute_ancestry($base_string, $options->{immediate}, $options);
	
	# Now use this temporary table to do the actual query.
	
	push @fields, 's.is_base';
	my $fields = join ', ', @fields;
	
	#$fields =~ s{t\.senpar_no}{t.immpar_no};
	
	my @filters = $taxonomy->taxon_filters($options, $tables);
	push @filters, $taxonomy->refno_filter($options, 'a');
	my $filters = join( q{ and }, @filters);
	$filters ||= '1=1';
	
	my $other_joins = $taxonomy->taxon_joins('t', $tables);
	$other_joins .= $taxonomy->ref_joins('a', $tables);
	
	$order_expr ||= 'ORDER BY t.lft';
	
	$sql = "SELECT $count_expr $fields
		FROM ancestry_temp as s JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on $auth_expr
			$other_joins
		WHERE $filters
		GROUP BY t.orig_no $order_expr $limit_expr\n";
    }
    
    elsif ( $rel eq 'crown' || $rel eq 'pan' || $rel eq 'stem' || $rel eq 'common' ||
	    $rel eq 'common_ancestor' )
    {
	$rel = 'common' if $rel eq 'common_ancestor';
	
	my $common_string = $taxonomy->find_common_taxa($base_string, $rel, $options);
	
	my $fields = join ', ', @fields;
	
	#$fields =~ s{t\.senpar_no}{t.immpar_no};
	
	my @filters = "t.orig_no in ($common_string)";
	push @filters, $taxonomy->taxon_filters($options, $tables);
	push @filters, $taxonomy->refno_filter($options, 'a');
	my $filters = join( q{ and }, @filters);
	
	my $other_joins = $taxonomy->taxon_joins('t', $tables);
	$other_joins .= $taxonomy->ref_joins('a', $tables);
	
	$order_expr ||= 'ORDER BY t.lft';
	
	$sql = "SELECT $count_expr $fields
		FROM $tree_table as t JOIN $auth_table as a on $auth_expr
			$other_joins
		WHERE $filters
		GROUP BY t.orig_no $order_expr $limit_expr\n";
    }
    
    elsif ( $rel eq 'all_taxa' || $rel eq 'all_records' )
    {
	my $fields = join ', ', @fields;
	
	my @filters = $taxonomy->taxon_filters($options, $tables);
	push @filters, $taxonomy->refno_filter($options, 'a');
	push @filters, '1=1' unless @filters;
	my $filters = join( q{ and }, @filters);
	
	my $other_joins = $taxonomy->taxon_joins('t', $tables);
	$other_joins .= $taxonomy->ref_joins('a', $tables);
	
	# If we are asked to order by created or modified date, override the query optimizer
	# because it makes the wrong choice.
	
	my $force = '';
	
	if ( $order_expr && $order_expr =~ /^order by a.(created|modified)/i )
	{
	    $force = " force index ($1)";
	}
	
	# $order_expr ||= 'ORDER BY if(t.lft > 0, 0, 1), t.lft';
	
	$sql = "SELECT $count_expr $fields
		FROM $tree_table as t JOIN $auth_table as a$force on $auth_expr
			$other_joins
		WHERE $filters $order_expr $limit_expr\n";
    }
    
    elsif ( $rel eq 'occs' || $rel eq 'specs' )
    {
	my $occs_table = $options->{table};
	my $tree_table = $taxonomy->{TREE_TABLE};
	my $lower_table = $taxonomy->{LOWER_TABLE};
	
	croak "you must include the option 'table' if you specify rel = 'occs' or rel = 'specs'" unless $occs_table;
	
	$taxonomy->generate_taxa_list($options->{table}, $options);
	
	$auth_expr = $options->{all_variants} ? 'a.orig_no = t.orig_no' :
	    'a.taxon_no = list.taxon_no';
	
	my @filters = $taxonomy->taxon_filters($options, $tables);
	push @filters, $taxonomy->range_filter($base_string);
	push @filters, $taxonomy->exclusion_filters($base_nos);
	push @filters, '1=1' unless @filters;
	
	my $filters = join( q{ and }, @filters);
	my $fields = join ', ', @fields;
	
	$fields =~ s/v.n_occs/sum(list.n_occs) as n_occs/;
	
	my $other_joins = $taxonomy->taxon_joins('t', $tables);
	$other_joins .= $taxonomy->ref_joins('a', $tables);
	
	$order_expr ||= 'ORDER BY t.lft';
	
	$sql = "SELECT $count_expr $fields
		FROM taxa_list as list JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on $auth_expr
			$other_joins
		WHERE $filters
		GROUP BY $group_expr $order_expr $limit_expr\n";
	
	$drop_table = 'taxa_list';
    }
    
    else
    {
	croak "list_taxa: invalid relationship '$rel'\n";
    }
    
    # Now execute the query and return the result.
    
    return $taxonomy->execute_query($sql, { record_type => 'taxa',
					    return_type => $return_type,
					    count => $options->{count},
					    debug_out => $options->{debug_out},
					    sql_strings => \@sql_strings,
					    drop_table => $drop_table,
					    base_nos => $base_nos,
					    copy_exclusions => $copy_exclusions } );
}

    
# find_common_taxa ( base_string, rel, options )
# 
# This routine returns a comma-separated taxon id string corresponding to one
# of the following relationships:
# 
#  - common
#  - crown
#  - pan
#  - stem
# 
# The paramter $base_string must be a comma-separated id string representing
# the base taxa for this query.  The parameter $rel must specify one of the
# above relationships.  The parameter $options is passed through from the
# calling routine.

sub find_common_taxa {
    
    my ($taxonomy, $base_string, $rel, $options) = @_;
    
    my $dbh = $taxonomy->{dbh};
    my ($ancestry, $common_index, $common_id);
    my ($crown_id, $crown_lft, $crown_rgt);
    my ($pan_id);
    
    my $ATTRS_TABLE = $taxonomy->{ATTRS_TABLE};
    my $TREE_TABLE = $taxonomy->{TREE_TABLE};
    
    # We start by computing the ancestry of the specified base taxa.  The
    # following call will select into the temporary table 'ancestry_temp' the
    # set of orig_no values representing the ancestors of the taxa identified by
    # $base_string.
    
    $taxonomy->compute_ancestry($base_string, undef, $options);
    
    # Now use this temporary table to query for the set of ancestral taxa.
    
    my $sql =  "SELECT t.orig_no, t.synonym_no, t.lft, t.rgt, s.is_base, v.extant_children
		FROM ancestry_temp as s JOIN $TREE_TABLE as t using (orig_no)
			JOIN $ATTRS_TABLE as v on v.orig_no = t.orig_no
		GROUP BY t.lft ORDER BY t.lft";
    
    $taxonomy->{sql_string} .= "$sql\n\n";
    
    if ( ref $options->{debug_out} eq 'CODE' )
    {
	&{$options->{debug_out}}("$sql\n");
    }
    
    $ancestry = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    # If no ancestors were found, we return just "0" which will lead to an
    # empty result.
    
    unless ( ref $ancestry && @$ancestry )
    {
	$taxonomy->add_warning('W_COMMON', "could not determine common ancestor");
	return "0";
    }
    
    # The next step is to find the common ancestor of the base taxa.  This
    # will be necessary for all of the relationships covered by this routine.
    
    # Start by finding the minimum and maximum tree sequence values for the base
    # taxa.
    
    my $min;
    my $max = 0;
    my $common_index;
    
    foreach my $t (@$ancestry)
    {
	if ( $t->{is_base} )
	{
	    $min = $t->{lft} if !defined $min or $t->{lft} < $min;
	    $max = $t->{lft} if !defined $max or $t->{lft} > $max;
	}
    }
    
    # Then find the index of the latest (most specific) taxon which encompasses all of
    # the base taxa and is not a junior synonym.
    
    foreach my $i (0..$#$ancestry)
    {
	if ( $ancestry->[$i]{lft} <= $min and $ancestry->[$i]{rgt} >= $max
	     and $ancestry->[$i]{orig_no} eq $ancestry->[$i]{synonym_no} )
	{
	    $common_index = $i;
	    $common_id = $ancestry->[$i]{orig_no};
	}
    }
    
    # If we couldn't find a common id for some reason, return "0" which will
    # generate an empty result.
    
    unless ( $common_id )
    {
	$taxonomy->add_warning('W_COMMON', "could not determine common ancestor");
	return "0";
    }
    
    # If the relationship was 'common', we are done.  Simply return the taxon
    # id of the common ancestor.
    
    if ( $rel eq 'common' )
    {
	return $common_id;
    }
    
    # If the relationship was 'crown' or 'stem', we must then search for the
    # crown group as a child of this common ancestor.  This is done by
    # querying for the highest-ranking subtaxon that itself has at least two
    # extant subtaxa.
    
    if ( $rel eq 'crown' || $rel eq 'stem' )
    {
	my $crown_sql = "
	SELECT t.orig_no, t.lft, t.rgt
	FROM $TREE_TABLE as tb JOIN $TREE_TABLE as t on t.lft between tb.lft and tb.rgt
		JOIN $ATTRS_TABLE as v on v.orig_no = t.orig_no and v.extant_children > 1
	WHERE tb.orig_no = $common_id and t.synonym_no = t.orig_no
	ORDER BY t.depth LIMIT 1";
	
	if ( ref $options->{debug_out} eq 'CODE' )
	{
	    &{$options->{debug_out}}("$crown_sql\n");
	}
	
	my $t = $dbh->selectrow_hashref($crown_sql);
	
	unless ( ref $t eq 'HASH' )
	{
	    $taxonomy->add_warning('W_COMMON', "could not determine crown group");
	    return "0";
	}
	
	$crown_id = $t->{orig_no};
	$crown_lft = $t->{lft};
	$crown_rgt = $t->{rgt};
	
	# If the relationship was 'crown', we can return the result now.
	
	if ( $rel eq 'crown' )
	{
	    return $crown_id;
	}
    }
    
    # If the relationship was 'pan' or 'stem', we need to scan up the ancestry
    # list starting with the parent of the crown group, and stop at the taxon
    # just before the first one we find that has more than one living
    # subtaxon.
    
    if ( $rel eq 'pan' || $rel eq 'stem' )
    {
	my @pan_list = reverse (0..$common_index-1);
	my $pan_id = $common_id;
	
	foreach my $i (@pan_list)
	{
	    last if $ancestry->[$i]{extant_children} > 1;
	    $pan_id = $ancestry->[$i]{orig_no};
	}
	
	if ( $rel eq 'pan' )
	{
	    return $pan_id;
	}
    }
}


# list_associated ( rel, base_nos, options )
# 
# List records associated with the specified taxa.  The option 'record_type' specifies which
# records to return, and can take any of the following values:
# 
# opinions	list opinions associated with the specified taxa
# refs		list references associated with the specified taxa
# taxa		list the specified taxa, grouped by reference and ref_type 

sub list_associated {
    
    my ($taxonomy, $rel, $base_nos, $options) = @_;
    
    # First check the arguments.
    
    $taxonomy->clear_warnings;
    $taxonomy->clear_sql;
    
    croak "list_refs: third argument must be a hashref if given"
	if defined $options && ref $options ne 'HASH';
    $options ||= {};
    
    my $record_type = $options->{record_type};
    my $return_type = lc $options->{return} || 'list';
    my $base_string;
    my $has_common_opt;
    
    croak "list_associated: second argument must be a valid relationship\n"
	unless defined $rel;
    
    croak "list_associated: bad value '$record_type' for 'record_type'\n"
	unless $RECORD_TYPE_VALUE{$record_type};
    
    unless ( $rel eq 'all_taxa' || $rel eq 'all_records' || $rel eq 'occs' || $rel eq 'specs' ||
	     ($base_string = $taxonomy->generate_id_string($base_nos)) )
    {
	return $return_type eq 'listref' ? [] : ();
    }
    
    # Check the options.  If an option for selecting references is present,
    # then we need to enabled 'all_variants' so that the particular variant
    # named in each reference is shown regardless of whether or not it is the
    # currently accepted one.
    
    foreach my $key ( keys %$options )
    {
	croak "list_associated: invalid option '$key'\n"
	    unless $STD_OPTION{$key} || $TAXON_OPTION{$key} || $ASSOC_OPTION{$key} ||
		$OP_OPTION{$key} || $REF_OPTION{$key};
    }
    
    # This is either redundant or too limited.  Whenever we are selecting based on reference
    # parameters, we need to show all variants. Yes??? $$$
    
    if ( $options->{reference_no} )
    {
	$options->{all_variants} = 1 unless $options->{current_only};
    }
    
    # Set up the query.
    
    my $inner_tables = { };
    
    my $count_expr = $options->{count} ? 'SQL_CALC_FOUND_ROWS' : '';
    my $limit_expr = $taxonomy->simple_limit($options);
    
    my %select;
    
    if ( $record_type eq 'opinions' )
    {
	my $type = $options->{op_type} || 'all';
	
	croak "list_associated: invalid value '$type' for 'op_type'" unless $OP_SELECT_VALUE{$type};
	
	if ( $type eq 'class' || $type eq 'taxonomy' )
	{
	    $select{ops_class} = 1;
	}
	
	else
	{
	    $select{ops_all} = 1;
	}
    }
    
    else
    {
	my @select = ref $options->{ref_type} eq 'ARRAY' ? @{$options->{ref_type}}
	           : $options->{ref_type}		 ? split(qr{\s*,\s*}, $options->{ref_type})
							 : 'taxonomy';
	
	foreach my $s (@select)
	{
	    next unless $s;
	    croak "list_associated: invalid value '$s' for 'select'" unless $REF_SELECT_VALUE{$s};
	    
	    if ( $s eq 'all' )
	    {
		$select{refs_auth} = 1;
		$select{refs_var} = 1 unless $options->{current_only};
		$select{refs_ops} = 1;
		$select{refs_occs} = 1;
		$select{refs_specs} = 1;
		$select{refs_colls} = 1;
	    }
	    
	    elsif ( $s eq 'taxonomy' )
	    {
		$select{refs_auth} = 1;
		$select{refs_class} = 1;
	    }
	    
	    else
	    {
		$select{"refs_$s"} = 1;
	    }
	}
    }
    
    my $dbh = $taxonomy->{dbh};
    my $refs_table = $taxonomy->{REFS_TABLE};
    my $auth_table = $taxonomy->{AUTH_TABLE};
    my $tree_table = $taxonomy->{TREE_TABLE};
    my $op_cache = $taxonomy->{OP_CACHE};
    my $op_table = $taxonomy->{OP_TABLE};
    
    $rel = 'current' if $rel eq 'self';
    $rel = 'all_taxa' if $rel eq 'all_records' && $options->{current_only};
    
    my $auth_expr = 'a.taxon_no = t.spelling_no';
    
    if ( $options->{all_variants} || $rel eq 'variants' || $rel eq 'all_records' || $select{refs_var} )
    {
	$inner_tables->{use_a} = 1;
	$rel = 'variants' if $rel eq 'exact' || $rel eq 'current';
	$rel = 'all_taxa' if $rel eq 'all_records';
	$auth_expr = 'a.orig_no = t.orig_no';
    }
    
    elsif ( $rel eq 'exact' )
    {
	$inner_tables->{use_a} = 1;
	$auth_expr = 'a.taxon_no = base.taxon_no';
    }
    
    # Set filter clauses based on the specified options
    
    my $taxon_joins = '';
    my $other_joins = '';
    my @inner_filters;
    my @sql_strings;
    
    if ( $rel eq 'exact' || $rel eq 'current' || $rel eq 'variants' || $rel eq 'self' )
    {
	push @inner_filters, "base.taxon_no in ($base_string)";
	push @inner_filters, $taxonomy->taxon_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->ref_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->opinion_filters($options, $inner_tables)
	    if $record_type eq 'opinions';
	
	$taxon_joins = "$auth_table as base JOIN $tree_table as t using (orig_no)\n";
	$taxon_joins .= $taxonomy->taxon_joins('t', $inner_tables);
    }
    
    elsif ( $rel eq 'accepted' || $rel eq 'senior' || $rel eq 'parent' || $rel eq 'senpar' || $rel eq 'immpar' )
    {
	push @inner_filters, "base.taxon_no in ($base_string)";
	push @inner_filters, $taxonomy->taxon_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->ref_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->opinion_filters($options, $inner_tables)
	    if $record_type eq 'opinions';
	
	if ( $rel eq 'parent' )
	{
	    $rel = $options->{immediate} ? 'immpar' : 'senpar';
	}
	
	my $rel_field = $rel eq 'senior' ? 'synonym_no' : $rel . '_no';
	
	$taxon_joins = "$auth_table as base JOIN $tree_table as tb using (orig_no)
		JOIN $tree_table as t on t.orig_no = tb.$rel_field\n";
	$taxon_joins .= $taxonomy->taxon_joins('t', $inner_tables);
    }
    
    elsif ( $rel eq 'synonyms' || $rel eq 'children' )
    {
	push @inner_filters, "base.taxon_no in ($base_string)";
	push @inner_filters, $taxonomy->taxon_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->exclusion_filters($base_nos);
	push @inner_filters, $taxonomy->ref_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->opinion_filters($options, $inner_tables)
	    if $record_type eq 'opinions';
	
	my ($sel_field, $rel_field);
	
	# Select the fields on which to query
	
	if ( $rel eq 'synonyms' )
	{
	    $rel_field = 'synonym_no';
	    $sel_field = 'synonym_no';
	}
	
	elsif ( $options->{immediate} )
	{
	    $rel_field = 'immpar_no';
	    $sel_field = 'orig_no';
	}
	
	else
	{
	    $rel_field = 'senpar_no';
	    $sel_field = 'synonym_no';
	}
	
	$taxon_joins = "$auth_table as base JOIN $tree_table as tb using (orig_no)
		JOIN $tree_table as t on t.$rel_field = tb.$sel_field\n";
	$taxon_joins .= $taxonomy->taxon_joins('t', $inner_tables);
    }
    
    elsif ( $rel eq 'all_children' )
    {
	push @inner_filters, "base.taxon_no in ($base_string)";
	push @inner_filters, $taxonomy->taxon_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->exclusion_filters($base_nos);
	push @inner_filters, $taxonomy->ref_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->opinion_filters($options, $inner_tables)
	    if $record_type eq 'opinions';
	
	if ( $options->{immediate} )
	{
	    $taxon_joins = "$auth_table as base JOIN $tree_table as tb using (orig_no)
		JOIN $tree_table as t on t.lft between tb.lft and tb.rgt\n";
	}
	
	else
	{
	    $taxon_joins = "$auth_table as base JOIN $tree_table as tb using (orig_no)
		JOIN $tree_table as tb2 on tb2.orig_no = tb.synonym_no
		JOIN $tree_table as t on t.lft between tb2.lft and tb2.rgt\n";
	}
	
	$taxon_joins .= $taxonomy->taxon_joins('t', $inner_tables);
    }
    
    elsif ( $rel eq 'all_parents' )
    {
	# First select into the temporary table 'ancestry_temp' the set of
	# orig_no values representing the ancestors of the taxa identified by
	# $base_string.
	
	$taxonomy->compute_ancestry($base_string, $options->{immediate}, $options);
	
	# Now use this temporary table to do the actual query.
	
	push @inner_filters, $taxonomy->taxon_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->ref_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->opinion_filters($options, $inner_tables)
	    if $record_type eq 'opinions';
	push @inner_filters, '1=1' unless @inner_filters;
	
	$taxon_joins = "ancestry_temp as base JOIN $tree_table as t using (orig_no)\n";
	$taxon_joins .= $taxonomy->taxon_joins('t', $inner_tables);
    }
    
    elsif ( $rel eq 'all_taxa' && $record_type eq 'opinions' )
    {
	push @inner_filters, $taxonomy->taxon_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->ref_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->opinion_filters($options, $inner_tables);
	push @inner_filters, '1=1' unless @inner_filters;
	
	$other_joins = $taxonomy->opinion_joins('o', $inner_tables);
	$other_joins .= $taxonomy->ref_joins('r', $inner_tables);
    }
    
    elsif ( $rel eq 'all_taxa' )
    {
	push @inner_filters, $taxonomy->taxon_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->ref_filters($options, $inner_tables);
	push @inner_filters, '1=1' unless @inner_filters;
	
	$other_joins = $taxonomy->ref_joins('r', $inner_tables);
    }
    
    elsif ( $rel eq 'occs' || $rel eq 'specs' )
    {
	my $occs_table = $options->{table};
	my $tree_table = $taxonomy->{TREE_TABLE};
	my $lower_table = $taxonomy->{LOWER_TABLE};
	
	croak "you must include the option 'table' if you specify rel = 'occs' or rel = 'specs'" unless $occs_table;
	
	$taxonomy->generate_taxa_list($occs_table, $options, \@sql_strings);
	
	$auth_expr = 'a.taxon_no = list.taxon_no' unless $options->{all_variants};
	
	push @inner_filters, $taxonomy->taxon_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->range_filter($base_string);
	push @inner_filters, $taxonomy->exclusion_filters($base_nos);
	push @inner_filters, $taxonomy->ref_filters($options, $inner_tables);
	push @inner_filters, $taxonomy->opinion_filters($options, $inner_tables);
	push @inner_filters, "1=1" unless @inner_filters;
	
	$taxon_joins = "taxa_list as list JOIN $tree_table as t using (orig_no)\n";
	$taxon_joins .= $taxonomy->taxon_joins('t', $inner_tables);
    }
    
    else
    {
	croak "invalid relationship '$rel'\n";
    }
    
    # Now we put the query together depending upon what data type was chosen
    # and also depending upon the value of the 'select' option.  We start with
    # the "inner query" which retrieves the necessary set of reference_no or
    # opinion_no values along with some columns that indicate what type of
    # reference or what type of opinion each one represents.  This may consist
    # either of a single SQL statement or a union of multiple ones.  Below, we
    # will wrap that in an "outer query" which retrieves the rest of the
    # information necessary to make up the result records.
    
    my ($sql);
    
    # If we were asked to return opinions, construct the query and then execute it.
    
    if ( $record_type eq 'opinions' )
    {
	$dbh->do("DROP TABLE IF EXISTS op_collect");
	
	my $temp = ''; $temp = 'TEMPORARY' unless $Web::DataService::ONE_PROCESS;
	my $engine = $rel eq 'all_taxa' ? 'engine=myisam' : 'engine=memory';
	
	$dbh->do("CREATE $temp TABLE op_collect (
		opinion_no int unsigned not null,
		opinion_type char(1) not null,
		taxon_no int unsigned not null,
		orig_no int unsigned not null,
		UNIQUE KEY (opinion_no)) $engine");
	
	push @inner_filters, 'o.opinion_no = t.opinion_no' if $rel eq 'all_taxa' && ! $select{ops_all};
	push @inner_filters, $taxonomy->refno_filter($options, 'o');
	push @inner_filters, $taxonomy->extra_filters($options);
	
	my $limit_applied;	# This is used to make sure that 'LIMIT' expression is not applied
                                # to both the outer and inner query inappropriately.
	
	my $rows_found;
	
	my $type = $select{ops_all}
	    ? "if(o.opinion_no = t.opinion_no, $TYPE_CLASS, if(o.suppress, $TYPE_SUPPRESSED, $TYPE_UNSEL))"
	    : $TYPE_CLASS;
	
	my $inner_filters = join q{ and }, @inner_filters;
	
	my $order_expr = $taxonomy->opinion_order($options, $rel);
	
	if ( ! $order_expr && $options->{record_order} && $options->{record_order} eq 'byref' )
	{
	    $order_expr = "ORDER BY o.reference_no, a.taxon_name";
	}
	
	# If the relationship is 'all_taxa', then we just go through the entire opinion cache
	# because that is the most efficient procedure.  If a limit expression was given, use
	# that here.
	
	if ( $rel eq 'all_taxa' )
	{
	    # If we are asked to order by created or modified date, override the query optimizer
	    # because it makes the wrong choice.
	    
	    my $force = '';
	    
	    if ( $order_expr && $order_expr =~ /^order by oo.(created|modified)/i )
	    {
		$force = " force index ($1)";
	    }
	    
	    my $query_core = "$op_cache as o
			JOIN $op_table as oo$force using (opinion_no)
			JOIN $auth_table as a on a.taxon_no = o.child_spelling_no
			JOIN $tree_table as t on t.orig_no = o.orig_no
			JOIN $refs_table as r on r.reference_no = o.reference_no
			$other_joins";
	    
	    # If the order and limit expressions can be applied here, as opposed to the outer
	    # query, do so.  NOTE: Currently, as of 2016-05-10, all of of the available orderings
	    # can be applied here.  If more orderings are added later, you should check to see if
	    # some of them need to be applied to the outer query instead.
	    
	    my $additional = 'ORDER BY NULL';		# If no explicit order was specified, it
                                                        # is most efficient by far to simply
                                                        # return the results in table order.
	    
	    # If necessary, replace "0" with a check for orderings that need to be applied to the
	    # outer query instead.
	    
	    unless ( 0 )
	    {
		$additional = $order_expr if $order_expr;
		$additional .= " $limit_expr" if $limit_expr;
		$limit_applied = 1 if $limit_expr;
	    }
	    
	    # Now execute the "inner query".
	    
	    $sql = "INSERT IGNORE INTO op_collect
		SELECT $count_expr o.opinion_no, $type as opinion_type, 
			o.child_spelling_no as taxon_no, t.orig_no
		FROM $query_core
		WHERE $inner_filters $additional";
	    
	    if ( ref $options->{debug_out} eq 'CODE' )
	    {
		&{$options->{debug_out}}("$sql\n");
	    }
	    
	    $dbh->do($sql);
	    # push @sql_strings, $sql;
	    
	    # If we were asked to count rows, then get the total number of rows found by this
	    # statement.
	    
	    if ( $options->{count} )
	    {
		($rows_found) = $dbh->selectrow_array("SELECT FOUND_ROWS()");
	    }
	}
	
	# Otherwise, we select the specified taxa and grab the corresponding classification opinions.
	
	else
	{
	    my $base_clause = '';
	    # $base_clause = $rel eq 'exact' ? 'and o.child_spelling_no = base.taxon_no' : '';
	    my $query_core = "$taxon_joins
			JOIN $op_cache as o on o.opinion_no = t.opinion_no $base_clause
			JOIN $op_table as oo ignore key (created,modified) on oo.opinion_no = t.opinion_no
			JOIN $refs_table as r on r.reference_no = o.reference_no
			JOIN $auth_table as a on a.taxon_no = o.child_spelling_no";
	    
	    # If the order expression can be applied here, as opposed to the outer query, do so.
	    # IMPORTANT: this is not possible if a result offset was specified and all opinions
	    # are called for, because we need to integrate the classification and
	    # non-classification opinions before applying the offset.  If there is a limit but no
	    # offset, we can apply it here.  ALSO: the same caveat applies to this section as to
	    # the 'all_taxa' section immediately above.
	    
	    my $additional;
	    
	    # If necessary, replace "0" with a check for orderings that need to be applied to the
	    # outer query instead.  If no ordering was specified, the default order is by
	    # taxonomic hierarchy since we are only selecting classification opinions at this
	    # point.  If we were asked to count rows, we must put off the ordering and limit
	    # until the "outer query".
	    
	    unless ( $options->{offset} && $select{ops_all} || 0 )
	    {
		$additional = $order_expr || "ORDER BY t.lft";
		$additional .= " $limit_expr" if $limit_expr;
	    }
	    
	    else
	    {
		$additional = 'ORDER BY NULL';
	    }
	    
	    # Now execte the "inner query for classification opinions".
	    
	    $sql = "INSERT IGNORE INTO op_collect
		SELECT $count_expr DISTINCT o.opinion_no, $type as opinion_type, 
			o.child_spelling_no as taxon_no, t.orig_no
		FROM $query_core
		WHERE $inner_filters $additional";
	    
	    if ( ref $options->{debug_out} eq 'CODE' )
	    {
		&{$options->{debug_out}}("$sql\n");
	    }
	    
	    $dbh->do($sql);
	    # push @sql_strings, $sql;
	    
	    # If we were asked to count rows, then get the total number of rows found by this
	    # statement.
	    
	    if ( $options->{count} )
	    {
		($rows_found) = $dbh->selectrow_array("SELECT FOUND_ROWS()");
	    }
	}
	
	# If we were asked for all opinions but not all taxa, we then re-select the relevant taxa
	# and grab all of the non-classification opinions too.  We need to do it this way because
	# otherwise this query would require a join condition disjunction on two separate indexes
	# (o.opinion_no = t.opinion_no or o.orig_no = t.orig_no) which the query optimizer cannot
	# handle.  Both MySQL and Mariadb 10.0 punt and try to compare every possible row which
	# results in a hideously inefficient query.
	
	if ( $select{ops_all} && $rel ne 'all_taxa' )
	{
	    my $query_core = "$taxon_joins
			JOIN $op_cache as o on o.orig_no = t.orig_no
			JOIN $op_table as oo ignore key (created,modified) on oo.opinion_no = o.opinion_no
			JOIN $refs_table as r on r.reference_no = o.reference_no
			JOIN $auth_table as a on a.taxon_no = o.child_spelling_no";
	    
	    my $additional;
	    
	    # If necessary, replace "0" with a check for orderings that need to be applied to the
	    # outer query instead.  If no ordering was specified, the default order is the same as
	    # the one applied to the "outer query" below.
	    
	    unless ( $options->{offset} && $select{ops_all} || 0 )
	    {
		$additional = $order_expr || "ORDER BY t.lft, if(o.opinion_no=t.opinion_no,0,1), o.pubyr desc";
		$additional .= " $limit_expr" if $limit_expr;
	    }
	    
	    else
	    {
		$additional = 'ORDER BY NULL';
	    }
	    
	    # Now execute the "inner query for non-classification opinions".  We have to add the
	    # condition "o.opinion_no <> t.opinion_no" because we need this result set to be
	    # disjoint from the one produced by the "inner query for classification opinions"
	    # above.  That is important so that we can accurately compute the rows_found count.
	    
	    $sql = "INSERT IGNORE INTO op_collect
		SELECT $count_expr DISTINCT o.opinion_no, $type as opinion_type, 
			o.child_spelling_no as taxon_no, t.orig_no
		FROM $query_core
		WHERE $inner_filters and o.opinion_no <> t.opinion_no $additional";
	    
	    if ( ref $options->{debug_out} eq 'CODE' )
	    {
		&{$options->{debug_out}}("$sql\n");
	    }
	    
	    $dbh->do($sql);
	    # push @sql_strings, $sql;
	    
	    if ( $options->{count} )
	    {
		my ($non_class_found) = $dbh->selectrow_array("SELECT FOUND_ROWS()");
		$rows_found += $non_class_found;
	    }
	}
	
	# At this point, we know the total number of rows found if we were asked to count them.
	
	$taxonomy->{sql_rowcount} = $rows_found if defined $rows_found;
	
	# Now we put together the "outer" query that takes the list of opinions from op_collect
	# and joins it with all of the tables necessary to produce the requested output fields.
	# We apply the order and limit clauses, even if those have been applied above, because if
	# all opinions were requested then the results generated so far may be larger than
	# requested because of the dual select into op_collect.
	
	my $outer_tables = { };
	
	my $fieldspec = $options->{fields} || 'OP_DATA';	
	my @fields = $taxonomy->generate_fields($fieldspec, $outer_tables);
	my $query_fields = join ', ', @fields;
	
	$query_fields = 'base.opinion_no' if $return_type eq 'id';
	
	$outer_tables->{oo} = 1 if $order_expr =~ /\boo[.]/;
	
	my $outer_joins = $taxonomy->opinion_joins('o', $outer_tables);
	
	# If no order expression was given, and rel was not 'all_taxa', order by taxonomic
	# hierarchy, then by classification opinions first, then by recency of publication.
	
	if ( $rel ne 'all_taxa' )
	{
	    $order_expr ||= "ORDER BY t.lft, if(opinion_type='C',0,1), o.pubyr desc";
	}
	
	my $additional = $order_expr || "ORDER BY NULL";
	$additional .= " $limit_expr" if $limit_expr and ! $limit_applied;
	
	$sql = "SELECT $count_expr $query_fields
		FROM op_collect as base JOIN $op_cache as o using (opinion_no)
			JOIN $tree_table as t on t.orig_no = base.orig_no
			LEFT JOIN $auth_table as a on a.taxon_no = o.child_spelling_no
			LEFT JOIN $auth_table as ap on ap.taxon_no = o.parent_spelling_no
			LEFT JOIN refs as r on r.reference_no = o.reference_no
		$outer_joins
		GROUP BY opinion_no $additional";
	
	return $taxonomy->execute_query( $sql, { record_type => $record_type,
						 return_type => $return_type,
						 sql_strings => \@sql_strings,
						 debug_out => $options->{debug_out},
						 count => 0 } );
    }
    
    # If we get to this point in the code, then we were asked to return either references or taxa.
    # We may need more than one query, depending upon the value of the 'select' parameter.  So we
    # build all of the relevant queries and then UNION them together.  For all values of rel other
    # than 'all_taxa', we construct the inner query based on the value of $taxon_joins computed
    # above.
    
    my @inner_query;
    
    $dbh->do("DROP TABLE IF EXISTS ref_collect");
    
    my $temp = ''; $temp = 'TEMPORARY' unless $Web::DataService::ONE_PROCESS;
    
    $dbh->do("CREATE $temp TABLE ref_collect (
		reference_no int unsigned not null,
		ref_type varchar(10),
		taxon_no int unsigned null,
		orig_no int unsigned null,
		auth_no int unsigned null,
		var_no int unsigned null,
		unclass_no int unsigned null,
		class_no int unsigned null,
		occurrence_no int unsigned null,
		specimen_no int unsigned null,
		collection_no int unsigned null,
		UNIQUE KEY (reference_no, ref_type, taxon_no, orig_no, unclass_no, class_no, 
			occurrence_no, specimen_no, collection_no)) engine=memory");
    
    if ( $select{refs_auth} || $select{refs_var} )
    {
	my @auth_filters = @inner_filters;
	push @auth_filters, $taxonomy->refno_filter($options, 'a');
	push @auth_filters, $taxonomy->extra_filters($options);
	
	my $inner_filters = join(q{ and }, @auth_filters);
	
	my $query_core = $rel eq 'all_taxa'
	    ? "$refs_table as r
			JOIN $auth_table as a using (reference_no)
			JOIN $tree_table as t using (orig_no)
			$other_joins"
	    : "$taxon_joins
			JOIN $auth_table as a on $auth_expr
			JOIN $refs_table as r on r.reference_no = a.reference_no";
	
	$sql = "INSERT IGNORE INTO ref_collect
		SELECT a.reference_no, if(a.taxon_no=t.spelling_no,$TYPE_AUTH,$TYPE_VAR) as ref_type,
			a.taxon_no, t.orig_no, if(a.taxon_no=t.spelling_no, a.taxon_no, null) as auth_no,
			nullif(a.taxon_no,t.spelling_no) as var_no, 
			null as unclass_no, null as class_no,
			null as occurrence_no, null as specimen_no, null as collection_no
		FROM $query_core
		WHERE $inner_filters
		GROUP BY a.reference_no, a.taxon_no";
	
	if ( ref $options->{debug_out} eq 'CODE' )
	{
	    &{$options->{debug_out}}("$sql\n");
	}
	
	$dbh->do($sql);
	# push @sql_strings, $sql;
    }
    
    if ( $select{refs_class} || $select{refs_ops} )
    {
	my @ops_filters = @inner_filters;
	push @ops_filters, $taxonomy->refno_filter($options, 'o');
	push @ops_filters, $taxonomy->extra_filters($options);
	
	my $inner_filters = join(q{ and }, @ops_filters);
	
	my $type = $select{refs_ops}
	    ? "if(o.opinion_no = t.opinion_no, $TYPE_CLASS, $TYPE_UNSEL)"
	    : $TYPE_CLASS;
	
	my @join_conditions = 'o.opinion_no = t.opinion_no';

	if ( $select{refs_ops} )
	{
	    push @join_conditions, 'o.orig_no = t.orig_no';
	}
	
	# my $join_condition = $select{refs_ops}
	#     ? 'o.opinion_no = t.opinion_no or o.orig_no = t.orig_no'
	#     : 'o.opinion_no = t.opinion_no';

	foreach my $join_condition ( @join_conditions )
	{
	    my $query_core = $rel eq 'all_taxa'
		? "$refs_table as r
			JOIN $op_cache as o using (reference_no)
			JOIN $auth_table as a on a.taxon_no = o.child_spelling_no
			JOIN $tree_table as t on t.orig_no = o.orig_no"
		: "$taxon_joins
			JOIN $op_cache as o on $join_condition
			JOIN $auth_table as a on a.taxon_no = o.child_spelling_no
			JOIN $refs_table as r on r.reference_no = o.reference_no";
	    
	    $sql = "INSERT IGNORE INTO ref_collect
		SELECT o.reference_no, $type as ref_type, o.child_spelling_no as taxon_no, t.orig_no,
			null as auth_no, null as var_no,
			max(if(o.opinion_no = t.opinion_no, null, o.opinion_no)) as unclass_no,
			max(if(o.opinion_no = t.opinion_no, o.opinion_no, null)) as class_no,
			null as occurrence_no, null as specimen_no, null as collection_no
		FROM $query_core
		WHERE $inner_filters
		GROUP BY o.reference_no, o.opinion_no";
	    
	    if ( ref $options->{debug_out} eq 'CODE' )
	    {
		&{$options->{debug_out}}("$sql\n");
	    }
	    
	    $dbh->do($sql);
	    # push @sql_strings, $sql;
	    
	    my $query_core_2 = $rel eq 'all_taxa'
		? "$refs_table as r
			JOIN $op_cache as o using (reference_no)
			JOIN $auth_table as a on a.orig_no = o.orig_no and a.reference_no = o.reference_no
			JOIN $tree_table as t on t.orig_no = o.orig_no"
		: "$taxon_joins
			JOIN $op_cache as o on $join_condition
			JOIN $auth_table as a on a.orig_no = o.orig_no and a.reference_no = o.reference_no
			JOIN $refs_table as r on r.reference_no = o.reference_no";
	    
	    $sql = "INSERT IGNORE INTO ref_collect
		SELECT o.reference_no, $type as ref_type, o.child_spelling_no as taxon_no, t.orig_no,
			null as auth_no, null as var_no,
			max(if(o.opinion_no = t.opinion_no, null, o.opinion_no)) as unclass_no,
			max(if(o.opinion_no = t.opinion_no, o.opinion_no, null)) as class_no,
			null as occurrence_no, null as specimen_no, null as collection_no
		FROM $query_core
		WHERE $inner_filters
		GROUP BY o.reference_no, o.opinion_no";
	    
	    if ( ref $options->{debug_out} eq 'CODE' )
	    {
		&{$options->{debug_out}}("$sql\n");
	    }
	    
	    $dbh->do($sql);
	    # push @sql_strings, $sql;
	}
    }
    
    if ( $select{refs_occs} )
    {
	my @occs_filters = grep { $_ !~ qr{^t.accepted_no = t.(synonym_no|orig_no)$} } @inner_filters;
	push @occs_filters, $taxonomy->refno_filter($options, 'm');
	push @occs_filters, $taxonomy->extra_filters($options);
	push @occs_filters, $taxonomy->occ_filters($options, 'm');
	
	my $inner_filters = join(q{ and }, @occs_filters);

	my $group_expr = 'm.reference_no, m.occurrence_no';
	$group_expr .= ', m.reid_no' if $options->{ident_select} &&
	    $options->{ident_select} eq 'all';
	
	my $query_core;
	
	if ( $rel eq 'all_taxa' )
	{
	    $query_core = "$refs_table as r
			JOIN $OCC_MATRIX as m using (reference_no)
			JOIN $tree_table as t using (orig_no)
			JOIN $auth_table as a on a.taxon_no = m.taxon_no"
	}
	elsif ( $rel eq 'occs' || $rel eq 'specs' )
	{
	    my $occs_table = $options->{table};
	    $query_core = "$occs_table as list
			JOIN $OCC_MATRIX as m using (occurrence_no)
			JOIN $tree_table as t on t.orig_no = m.orig_no
			JOIN $auth_table as a on a.taxon_no = m.taxon_no
			JOIN $refs_table as r on r.reference_no = m.reference_no";
	}
	else
	{
	    $query_core = " $taxon_joins
			JOIN $OCC_MATRIX as m on m.orig_no = t.orig_no
			JOIN $auth_table as a on a.taxon_no = m.taxon_no
			JOIN $refs_table as r on r.reference_no = m.reference_no";
	}
	
	$sql = "INSERT IGNORE INTO ref_collect
		SELECT m.reference_no, $TYPE_OCC as ref_type, m.taxon_no, t.orig_no, null as auth_no,
			null as var_no, null as unclass_no, null as class_no, m.occurrence_no, 
			null as specimen_no, null as collection_no
		FROM $query_core
		WHERE $inner_filters
		GROUP BY $group_expr";
       
	if ( ref $options->{debug_out} eq 'CODE' )
	{
	    &{$options->{debug_out}}("$sql\n");
	}
	
	$dbh->do($sql);
	# push @sql_strings, $sql;
    }
    
    if ( $select{refs_specs} )
    {
	my @occs_filters = grep { $_ !~ qr{^t.accepted_no = t.(synonym_no|orig_no)$} } @inner_filters;
	push @occs_filters, $taxonomy->refno_filter($options, 'ss');
	push @occs_filters, $taxonomy->extra_filters($options);
	push @occs_filters, $taxonomy->occ_filters($options, 'ss');
	
	my $inner_filters = join(q{ and }, @occs_filters);
	
	my $query_core;
	
	if ( $rel eq 'all_taxa' )
	{
	    $query_core = "$refs_table as r
			JOIN $SPEC_MATRIX as ss using (reference_no)
			JOIN $tree_table as t on t.orig_no = ss.orig_no
			JOIN $auth_table as a on a.taxon_no = ss.taxon_no";
	}
	elsif ( $rel eq 'occs' )
	{
	    my $occs_table = $options->{table};
	    $query_core = "$occs_table as list
			JOIN $SPEC_MATRIX as ss using (occurrence_no)
			JOIN $tree_table as t on t.orig_no = ss.orig_no
			JOIN $auth_table as a on a.taxon_no = ss.taxon_no
			JOIN $refs_table as r on r.reference_no = ss.reference_no";
	}
	elsif ( $rel eq 'specs' )
	{
	    my $specs_table = $options->{table};
	    $query_core = "$specs_table as list
			JOIN $OCC_MATRIX as m using (occurrence_no)
			JOIN $SPEC_MATRIX as ss using (specimen_no)
			JOIN $tree_table as t on t.orig_no = ss.orig_no
			JOIN $auth_table as a on a.taxon_no = ss.taxon_no
			JOIN $refs_table as r on r.reference_no = ss.reference_no";
	}
	else
	{
	    $query_core = " $taxon_joins
			JOIN $SPEC_MATRIX as ss on ss.orig_no = t.orig_no
			JOIN $auth_table as a on a.taxon_no = ss.taxon_no
			JOIN $refs_table as r on r.reference_no = ss.reference_no";
	}
	
	$sql = "INSERT IGNORE INTO ref_collect
		SELECT ss.reference_no, $TYPE_SPEC as ref_type, ss.taxon_no, t.orig_no, null as auth_no,
			null as var_no, null as unclass_no, null as class_no, null as occurrence_no, 
			ss.specimen_no, null as collection_no
		FROM $query_core
		WHERE $inner_filters
		GROUP BY ss.reference_no, ss.specimen_no";
	
	if ( ref $options->{debug_out} eq 'CODE' )
	{
	    &{$options->{debug_out}}("$sql\n");
	}
	
	$dbh->do($sql);
	# push @sql_strings, $sql;
    }
    
    if ( $select{refs_colls} && $record_type ne 'taxa' )
    {
	my @colls_filters = grep { $_ !~ qr{^t.accepted_no = t.(synonym_no|orig_no)$} } @inner_filters;
	push @colls_filters, $taxonomy->refno_filter($options, 'c');
	push @colls_filters, $taxonomy->extra_filters($options);
	push @colls_filters, $taxonomy->occ_filters($options, 'm');
	
	my $inner_filters = join(q{ and }, @colls_filters);
	
	my $query_core;
	
	if ( $rel eq 'all_taxa' )
	{
	    $query_core = "$refs_table as r
			JOIN $COLL_MATRIX as c using (reference_no)";
	}
	elsif ( $rel eq 'occs' )
	{
	    my $occs_table = $options->{table};
	    $query_core = "$occs_table as list
			JOIN $OCC_MATRIX as m using (occurrence_no)
			JOIN $COLL_MATRIX as c using (collection_no)
			JOIN $tree_table as t on t.orig_no = m.orig_no
			JOIN $refs_table as r on r.reference_no = c.reference_no";
	}
	else
	{
	    $query_core = " $taxon_joins
			JOIN $OCC_MATRIX as m on m.orig_no = t.orig_no
			JOIN $COLL_MATRIX as c using (collection_no)
			JOIN $refs_table as r on r.reference_no = c.reference_no";	
	}
	
	$sql = "INSERT IGNORE INTO ref_collect
		SELECT c.reference_no, $TYPE_PRIMARY as ref_type, null as taxon_no, null as orig_no,
			null as auth_no, null as var_no, null as unclass_no, null as class_no,
			null as occurrence_no, null as specimen_no, c.collection_no
		FROM $query_core
		WHERE $inner_filters
		GROUP BY c.reference_no, c.collection_no";
	
	if ( ref $options->{debug_out} eq 'CODE' )
	{
	    &{$options->{debug_out}}("$sql\n");
	}
	
	$dbh->do($sql);
	# push @sql_strings, $sql;
    }
    
    # Now construct the full query using what we constructed above as a subquery.
    
    # my $inner_query = join("\nUNION ", @inner_query);
    # $inner_query .= " ORDER BY NULL" if $rel eq 'all_taxa';
    
    if ( $record_type eq 'taxa' )
    {
	my $outer_tables = { };
	
	my $fieldspec = $options->{fields} || 'REFTAXA_DATA';	
	my @fields = $taxonomy->generate_fields($fieldspec, $outer_tables);
	my $query_fields = join ', ', @fields;
	
	# If $rel is 'all_taxa', then the ref_filters have already been applied to the inner
	# query.  Otherewise, we apply them to the outer query.
	
	my @outer_filters;
	
	push @outer_filters, $taxonomy->ref_filters($options, $outer_tables)
	    unless $rel eq 'all_taxa';
	push @outer_filters, "1=1" unless @outer_filters;
	my $outer_filters = join q{ and }, @outer_filters;
	
	croak "list_associated: the option { return => 'id' } is not compatible with 'list_reftaxa'\n"
	    if $return_type eq 'id';
	
	my $order_expr = $taxonomy->taxon_order($rel, 'assoc', $options, $outer_tables) || 
	    "ORDER BY r.author1last, r.author1init, r.author2last, r.author2init, r.reference_no, a.taxon_name";
	my $outer_joins = $taxonomy->taxon_joins('t', $outer_tables);
	$outer_joins .= $taxonomy->ref_joins('r', $outer_tables);
	
	$sql = "SELECT $count_expr $query_fields
		FROM ref_collect as base
			LEFT JOIN refs as r using (reference_no)
			LEFT JOIN $tree_table as t using (orig_no)
			LEFT JOIN $auth_table as a using (taxon_no)
			$outer_joins
		WHERE $outer_filters
		GROUP BY base.reference_no, a.taxon_no $order_expr $limit_expr";
	
	return $taxonomy->execute_query( $sql, { record_type => $record_type, 
						 return_type => $return_type,
						 sql_strings => \@sql_strings,
						 debug_out => $options->{debug_out},
						 drop_table => 'ref_collect',
						 count => $options->{count} } );
    }
    
    else	# record_type eq 'refs'
    {
	my $outer_tables = { };
	
	my $fieldspec = $options->{fields} || 'REF_DATA';
	my @fields = $taxonomy->generate_fields($fieldspec, $outer_tables);
	push @fields, "group_concat(distinct ref_type) as ref_type";
	my $query_fields = join ', ', @fields;
	
	$query_fields = 'base.reference_no' if $return_type eq 'id';
	
	# If $rel is 'all_taxa', then the ref_filters have already been applied to the inner
	# query.  Otherewise, we apply them to the outer query.
	
	my @outer_filters;
	
	push @outer_filters, $taxonomy->ref_filters($options, $outer_tables)
	    unless $rel eq 'all_taxa';
	push @outer_filters, "1=1" unless @outer_filters;
	my $outer_filters = join q{ and }, @outer_filters;
	
	my $order_expr = $taxonomy->ref_order($options, $outer_tables);
	
	$order_expr ||= 'ORDER BY r.author1last, r.author1init, r.author2last, r.author2init, r.reference_no' unless $rel eq 'all_taxa';
	$order_expr ||= 'ORDER BY NULL';
	
	# my $outer_joins = $taxonomy->ref_joins('r', $outer_tables);
	
	$sql = "SELECT $count_expr $query_fields
		FROM ref_collect as base JOIN refs as r using (reference_no)
		WHERE $outer_filters
		GROUP BY reference_no $order_expr $limit_expr";
	
	return $taxonomy->execute_query( $sql, { record_type => $record_type, 
						 return_type => $return_type,
						 sql_strings => \@sql_strings,
						 debug_out => $options->{debug_out},
						 drop_table => 'ref_collect',
						 count => $options->{count} } );
    }
}


# generate_taxa_list ( occs_table, options, sql_listref )
# 
# Given the name of a table of occurrences, generate a list of corresponding
# taxa.  The occurrence table will typically be a temporary table, so should
# be opened only once in any SQL expression executed by this function because
# of the longstanding limitation in MySQL/MariaDB.  The second argument should
# be the options hashref that was passed to the caller.  The options 'exact'
# and 'higher' determine exactly how this list should be generated.
# 
# The third argument should be a reference to a list; all of the generated SQL
# strings are appended to this list, for debugging purposes.
# 
# The new table will be a temporary table named 'taxa_list'.

sub generate_taxa_list {
    
    my ($taxonomy, $occs_table, $options, $sql_listref) = @_;
    
    my $dbh = $taxonomy->{dbh};
    my $tree_table = $taxonomy->{TREE_TABLE};
    my $lower_table = $taxonomy->{LOWER_TABLE};
    
    my $sql;
    
    # Start by creating a temporary table to hold the list of taxa.  The
    # caller is responsible for dropping this table when no longer needed.
    # But just in case that didn't happen, we drop-if-exists first.  The
    # n_occs field records how many occurrences go with a particular taxon.
    
    $dbh->do("DROP TABLE IF EXISTS taxa_list");
    
    my $temp = ''; $temp = 'TEMPORARY' unless $Web::DataService::ONE_PROCESS;
    
    $dbh->do("CREATE $temp TABLE taxa_list (
		taxon_no int unsigned not null,
		orig_no int unsigned not null,
		n_occs int unsigned not null,
		primary key (taxon_no, orig_no)) engine=memory");
    
    # If the option 'exact' was given, then select the exact taxon identified
    # for each occurrence even if it is not the currently accepted version of
    # this taxonomic name.  For completeness, we also add the currently
    # accepted versions.
    
    if ( $options->{exact} )
    {
	# Get the exact taxon identifications
	
	$sql = "INSERT IGNORE INTO taxa_list
		SELECT taxon_no, orig_no, count(*)
		FROM $occs_table GROUP BY taxon_no";

	if ( ref $options->{debug_out} eq 'CODE' )
	{
	    &{$options->{debug_out}}("$sql\n");
	}
	
	$dbh->do($sql);
	# push @$sql_listref, $sql;
	
	# Add the accepted names, where they differ from the identified names (note
	# the WHERE clause).
	
	$sql = "INSERT IGNORE INTO taxa_list
		SELECT t.spelling_no, t.orig_no, count(*)
		FROM $occs_table as ot JOIN $tree_table as t1 using (orig_no)
			JOIN $tree_table as t on t.orig_no = t1.accepted_no
		WHERE t.spelling_no <> ot.taxon_no
		GROUP BY t.orig_no
		ON DUPLICATE KEY UPDATE n_occs = n_occs + values(n_occs)";
	
	if ( ref $options->{debug_out} eq 'CODE' )
	{
	    &{$options->{debug_out}}("$sql\n");
	}
	
	$dbh->do($sql);
	# push @$sql_listref, $sql;
    }
    
    # Otherwise, just get the accepted names.
    
    else
    {
	$sql = "INSERT IGNORE INTO taxa_list
		SELECT t.spelling_no, t.orig_no, count(*)
		FROM $occs_table JOIN $tree_table as t1 using (orig_no)
			JOIN $tree_table as t on t.orig_no = t1.accepted_no
		GROUP BY t.orig_no";
	
	if ( ref $options->{debug_out} eq 'CODE' )
	{
	    &{$options->{debug_out}}("$sql\n");
	}
	
	$dbh->do($sql);
	# push @$sql_listref, $sql;
    }
    
    # In either case, for all taxa which are not genera but have associated
    # genera, add those genera to the list.  This would include occurrences
    # identified to the species or subgenus level.
    
    $sql = "INSERT IGNORE INTO taxa_list
		SELECT t.spelling_no, t.orig_no, count(*)
		FROM $occs_table as list JOIN $lower_table as pl using (orig_no)
			JOIN $tree_table as t on t.orig_no = pl.genus_no
		WHERE pl.genus_no is not null and pl.genus_no <> pl.orig_no
		GROUP BY t.orig_no
		ON DUPLICATE KEY UPDATE n_occs = n_occs + values(n_occs)";
    
    if ( ref $options->{debug_out} eq 'CODE' )
    {
	&{$options->{debug_out}}("$sql\n");
    }
    
    $dbh->do($sql);
    # push @$sql_listref, $sql;
    
    # Same for subgenera.
    
    $sql = "INSERT IGNORE INTO taxa_list
		SELECT t.spelling_no, t.orig_no, count(*)
		FROM $occs_table JOIN $lower_table as pl using (orig_no)
			JOIN $tree_table as t on t.orig_no = pl.subgenus_no
		WHERE pl.subgenus_no is not null and pl.subgenus_no <> pl.orig_no
		GROUP BY t.orig_no
		ON DUPLICATE KEY UPDATE n_occs = n_occs + values(n_occs)";
    
    if ( ref $options->{debug_out} eq 'CODE' )
    {
	&{$options->{debug_out}}("$sql\n");
    }
    
    $dbh->do($sql);
    # push @$sql_listref, $sql;
    
    # If the option 'higher' was given, then expand the list by adding higher
    # taxa.  We add all the way up the tree, but this list will typically be
    # trimmed later by the filters generated by the calling function (i.e. if
    # a base taxon or taxonomic rank range was originally specified).
    
    if ( $options->{higher} )
    {
	$taxonomy->add_ancestry('taxa_list');
	
	push @$sql_listref, "# call compute_ancestry_2";
	
	$sql = "UPDATE taxa_list as tl JOIN $tree_table as t using (orig_no)
		SET tl.taxon_no = t.spelling_no
		WHERE tl.taxon_no = 0";
	
	if ( ref $options->{debug_out} eq 'CODE' )
	{
	    &{$options->{debug_out}}("$sql\n");
	}
	
	$dbh->do($sql);
	# push @$sql_listref, $sql;
    }
    
    my $a = 1;	# we can stop here when debugging
}


sub execute_query {
    
    my ($taxonomy, $sql, $params) = @_;
    
    my $dbh = $taxonomy->{dbh};
    my $return_type = $params->{return_type};
    my $record_type = $params->{record_type};
    my ($result_list, $sth);
    
    my ($package, $filename, $line) = caller;
    
    if ( ref $params->{sql_strings} eq 'ARRAY' )
    {
	$taxonomy->{sql_string} = join "\n\n", @{$params->{sql_strings}}, $sql;
    }
    
    else
    {
	$taxonomy->{sql_string} = $sql;
    }
    
    if ( $taxonomy->{test_mode} )
    {
	print STDERR "$sql\n\n";
	return;
    }
    
    elsif ( ref $params->{debug_out} eq 'CODE' )
    {
	&{$params->{debug_out}}($taxonomy->{sql_string});
    }
    
    try
    {
	if ( $return_type eq 'list' || $return_type eq 'listref' )
	{
	    $result_list = $dbh->selectall_arrayref($sql, { Slice => {} });
	}
	
	elsif ( $return_type eq 'id' )
	{
	    $result_list = $dbh->selectcol_arrayref($sql);
	}
	
	elsif ( $return_type eq 'stmt' )
	{
	    $sth = $dbh->prepare($sql);
	    $sth->execute();
	}
	
	else
	{
	    croak "invalid value '$return_type' for 'return_type'\n";
	}
	
	($taxonomy->{sql_rowcount}) = $dbh->selectrow_array("SELECT FOUND_ROWS()")
	    if $params->{count};
    }
    
    catch
    {
	$_ =~ s{ \s at \s / (.*) line \s+ \d* }{ at $filename line $line}xs;
	die $_;
    }
    
    finally
    {
	$dbh->do("DROP TABLE IF EXISTS $params->{drop_table}") if $params->{drop_table};
    };
    
    if ( $return_type eq 'list' || $return_type eq 'listref' )
    {
	$return_type ||= [];
	
	my $package = $RECORD_BLESS{$record_type};
	
	map { bless $_, $package } @$result_list;
	
	if ( $params->{base_nos} && ref $params->{base_nos} eq 'ARRAY' &&
	     ref $params->{base_nos}[0] && $params->{copy_exclusions} )
	{
	    $taxonomy->copy_exclusions($result_list, $params->{base_nos});
	}
	
	return $result_list if $return_type eq 'listref';
	return @$result_list;
    }
    
    elsif ( $return_type eq 'id' )
    {
	$result_list ||= [];
	return @$result_list;
    }
    
    elsif ( $return_type eq 'stmt' )
    {
	return $sth;
    }
    
    else
    {
	croak "invalid return type '$return_type'\n";
    }
}


sub taxa_opinions {

    my ($taxonomy, $rel, $base_nos, $options) = @_;
    
    $options ||= {};
    $options->{record_type} = 'opinions';
    
    return $taxonomy->list_associated($rel, $base_nos, $options);
}


sub taxa_refs {
    
    my ($taxonomy, $rel, $base_nos, $options) = @_;
    
    $options ||= {};
    $options->{record_type} = 'refs';
    
    return $taxonomy->list_associated($rel, $base_nos, $options);
}


sub refs_taxa {

    my ($taxonomy, $rel, $base_nos, $options) = @_;
    
    $options ||= {};
    $options->{record_type} = 'taxa';
    
    return $taxonomy->list_associated($rel, $base_nos, $options);
}


sub refs_ops {
    
    my ($taxonomy, $rel, $base_nos, $options) = @_;
    
    $options ||= {};
    $options->{record_type} = 'opinions';
    $options->{record_order} = 'byref';
    
    return $taxonomy->list_associated($rel, $base_nos, $options);
}


sub list_opinions {
    
    my ($taxonomy, $opinion_nos, $options) = @_;
    
    $options ||= {};
    $options->{record_type} = 'opinions';
    $options->{opinion_no} = $opinion_nos;
    
    return $taxonomy->list_associated('all_taxa', undef, $options);
}
    
    
sub resolve_names {

    my ($taxonomy, $names, $options) = @_;
    
    # Check the arguments.
    
    $taxonomy->clear_warnings;
    $taxonomy->clear_sql;
    
    croak "resolve_names: second argument must be a hashref"
	if defined $options && ref $options ne 'HASH';
    $options ||= {};
    
    my $return_type = lc $options->{return} || 'list';
    
    foreach my $key ( keys %$options )
    {
	croak "resolve_names: invalid option '$key'\n"
	    unless $STD_OPTION{$key} || $TAXON_OPTION{$key} || $RESOLVE_OPTION{$key};
    }
    
    # Check for option 'common'.
    
    my ($scientific, $common) = ('', '');
    
    if ( $options->{common} )
    {
	my @lang;
	
	foreach my $l ( ref $options->{common} eq 'ARRAY' ? 
			@{$options->{common}} :
			$options->{common} )
	{
	    if ( $l eq 'S' )
	    {
		$scientific = 1;
	    }
	    else
	    {
		push @lang, "'$l'";
	    }
	}
	
	$common = join(',', @lang);
    }
    
    else
    {
	$scientific = 1;
    }
    
    # Generate a template query that will be able to find a name.
    
    my $tables = {};
    my @fields = $taxonomy->generate_fields($options->{fields} || 'NEW_SEARCH', $tables);
    my @filters = $taxonomy->taxon_filters($options, $tables);
    push @filters, 's.is_current' if ($options->{status} && $options->{status} eq 'valid' || $options->{current});
    
    my $fields = join q{, }, @fields;
    my $joins = $taxonomy->taxon_joins('t', $tables);
    
    my $sql_base = "
	SELECT $fields
	FROM taxon_search as s join taxon_trees as t using (orig_no)
		join taxon_attrs as v using (orig_no)
		join authorities as a using (taxon_no)
	WHERE ";
	
    my $sql_order = "GROUP BY s.taxon_no ORDER BY v.n_occs desc, s.is_current desc, s.is_exact desc, t.lft asc";
    
    my $sql_limit = $options->{all_names} ? "LIMIT 1000" : "LIMIT 1";
    
    # Then split the argument into a list of distinct names to interpret.
    
    my @names;
    
    if ( ref $names eq 'ARRAY' )
    {
	foreach my $n (@$names)
	{
	    push @names, $taxonomy->lex_namestring($n, $options);
	}
    }

    else
    {
	@names = $taxonomy->lex_namestring($names, $options);
    }
    
    my @result;
    my (%base);
    
    # print STDERR "NAMES:\n\n";
    
    # foreach my $n (@names)
    # {
    # 	print STDERR "$n\n";
    # }
    
    # print STDERR "\n";
    # return;
    
    my $dbh = $taxonomy->{dbh};
    
  NAME:
    foreach my $n ( @names )
    {
	# If the name ends in ':', then it will be used as a base for
	# subsequent lookups.
	
	if ( $n =~ qr{ (.*) [:] $ }xs )
	{
	    my $base_name = $1;
	    
	    # If the base name itself contains a ':', then lookup the base
	    # using everything before the last ':'.  If nothing is found, then
	    # there must have been a bad name somewhere in the prefix.
	    
	    my ($prefix_base, $name);
	    
	    if ( $base_name =~ qr{ (.*) [:] (.*) }xs )
	    {
		$prefix_base = $base{$1};
		$name = $2;
		
		next NAME unless $prefix_base;
	    }
	    
	    else
	    {
		$name = $base_name;
	    }
	    
	    # Then try to see if the name matches an actual taxon.  If so,
	    # save it.
	    
	    if ( my $base = $taxonomy->lookup_base($name, $prefix_base, $options) )
	    {
		$base{$base_name} = $base;
	    }
	    
	    # Otherwise, the base will be undefined which will cause
	    # subsequent lookups to fail.
	    
	    # Now go on to the next entry.
	    
	    next NAME;
	}
	
	# Otherwise, this entry represents a name to resolve.  If it
	# starts with '^', then set the 'exclude' flag.
	
	my $exclude;
	$exclude = 1 if $n =~ s{^\^}{};
	
	# If the name contains a prefix, split it off and look up the base.
	# If no base was found, then the base must have included a bad name.
	# In that case, skip this entry.
	
	my $base_n = $n;
	my $range_clause;
	my $prefix_rank = 25;	# an arbitrary high number, happens to equal 'unranked'
	my @n_result;
	
	if ( $n =~ qr{ (.*) [:] (.*) }xs )
	{
	    my $prefix_base = $base{$1};
	    $n = $2;
	    
	    if ( ref $prefix_base && $prefix_base->{lft} > 0 && $prefix_base->{rgt} > 0 )
	    {
		$range_clause = 't.lft between '. $prefix_base->{lft} . ' and ' . $prefix_base->{rgt};
		$prefix_rank = $prefix_base->{taxon_rank} if $prefix_base->{taxon_rank};
	    }
	    
	    elsif ( defined $prefix_base && $prefix_base =~ qr{ ^ t[.]lft }xs )
	    {
		$range_clause = $prefix_base
	    }
	    
	    else
	    {
		next NAME;
	    }
	}
	
	$n =~ s{[.]}{% }g;
	
	if ( $scientific &&
	     $n =~ qr{ ^ ( [a-z_%]+ )
			    (?: \s+ \( ( [a-z_%]+ ) \s* \) )?
			    (?: \s+    ( [a-z_%]+ )    )?
			    (?: \s+    ( [a-z_%]+ )    )? $ }xsi )
	{
	    my $main = $1;
	    my $subgenus = $2;
	    my $species = $3;
	    $species .= " $4" if $4;
	    
	    my @clauses = @filters;
	    
	    unless ( $n =~ /[a-z]/i )
	    {
		$taxonomy->add_warning('W_BAD_NAME', "The name '$n' is not valid, it must have at least one letter");
		next NAME;
	    }
	    
	    if ( $species )
	    {
		my $genus_clause;
		
		if ( $subgenus && $subgenus !~ /%/ )
		{
		    my $quoted = $dbh->quote($subgenus);
		    push @clauses, "s.genus like $quoted";
		    $genus_clause = 1;
		}
		
		elsif ( $main && $main !~ /%/ )
		{
		    my $quoted = $dbh->quote($main);
		    push @clauses, "s.genus like $quoted";
		    $genus_clause = 1;
		}
		
		if ( $subgenus )
		{
		    my $quoted = $dbh->quote("$main ($subgenus) %");
		    push @clauses, "s.full_name like $quoted";
		}
		
		elsif ( ! $genus_clause )
		{
		    my $quoted = $dbh->quote("$main %");
		    my $quoted2 = $dbh->quote("% ($main) %");
		    push @clauses, "(s.full_name like $quoted or s.full_name like $quoted2)";
		}
		
		my $q_species = $dbh->quote($species);
		push @clauses, "s.taxon_name like $q_species and s.taxon_rank < 4";
	    }
	    
	    elsif ( $subgenus eq '%' )
	    {
		my $quoted = $dbh->quote($main);
		push @clauses, "s.genus like $quoted and s.taxon_rank = 4";
	    }
	    
	    elsif ( $subgenus )
	    {
		my $quoted = $dbh->quote($main);
		my $q_sub = $dbh->quote($subgenus);
		push @clauses, "s.genus like $quoted and s.taxon_name like $q_sub and s.taxon_rank = 4";
	    }
	    
	    elsif ( $prefix_rank eq '5' )
	    {
		my $quoted = $dbh->quote($main);
		push @clauses, "s.taxon_name like $quoted and s.taxon_rank < 5";
	    }
	    
	    else
	    {
		my $quoted = $dbh->quote($main);
		push @clauses, "s.taxon_name like $quoted and s.taxon_rank >= 4";
	    }
	    
	    push @clauses, "($range_clause)" if $range_clause;
	    push @clauses, "common = ''";
	    
	    my $sql = $sql_base . join(' and ', @clauses) . "\n" . $sql_order . " " . $sql_limit;
	    
	    $taxonomy->{sql_string} .= $sql;
	    $taxonomy->{sql_string} .= "\n\n";
	    
	    if ( ref $options->{debug_out} eq 'CODE' )
	    {
		&{$options->{debug_out}}("$sql\n");
	    }
	    
	    my $this_result = $dbh->selectall_arrayref($sql, { Slice => {} });
	    
	    foreach my $r ( @$this_result )
	    {
		bless $r, 'PBDB::Taxon';
		$r->{exclude} = 1 if $exclude;
		push @n_result, $return_type eq 'id' ? $r->{taxon_no} : $r;
	    }
	    
	    if ( ref $this_result->[0] )
	    {
		$base{$base_n} = $this_result->[0];
	    }
	}
	
	elsif ( $scientific )
	{
	    $taxonomy->add_warning('W_BAD_NAME', "The name '$n' is not valid, " .
				   "it does not match the pattern for a scientific name");
	    next NAME unless $common;
	}
	
	if ( $common && $n =~ qr{ ^ [A-Za-z %._-]+ $ }xs )
	{
	    my @clauses = @filters;
	    
	    $n =~ s/^\s+//;
	    $n =~ s/\s+$//;
	    
	    unless ( $n =~ /[a-z]/i )
	    {
		$taxonomy->add_warning('W_BAD_NAME', "The name '$n' is not valid, it must have at least one letter");
		next NAME;
	    }
	    
	    my $quoted = $dbh->quote($n);
	    push @clauses, "s.taxon_name like $quoted and s.common in ($common)";
	    push @clauses, "($range_clause)" if $range_clause;
	    
	    my $sql = $sql_base . join(' and ', @clauses) . "\n" . $sql_order . " " . $sql_limit;
	    
	    if ( ref $options->{debug_out} eq 'CODE' )
	    {
		&{$options->{debug_out}}("$sql\n");
	    }
	    
	    my $this_result = $dbh->selectall_arrayref($sql, { Slice => {} });
	    
	    foreach my $r ( @$this_result )
	    {
		bless $r, 'PBDB::Taxon';
		$r->{exclude} = 1 if $exclude;
		push @n_result, $return_type eq 'id' ? $r->{taxon_no} : $r;
	    }
	    
	    if ( ref $this_result->[0] )
	    {
		$base{$base_n} = $this_result->[0];
	    }
	}
	
	elsif ( $common )
	{
	    $taxonomy->add_warning('W_BAD_NAME', "The name '$n' is not valid, " .
				   "it contains an invalid character");
	    next NAME unless @n_result;
	}
	
	if ( @n_result )
	{
	    push @result, @n_result; # $$$
	}
	
	else
	{
	    my $phrase = $options->{current} ? 'the currently accepted variant of any name' : 'any name';
	    
	    $taxonomy->add_warning('W_NO_MATCH', "The name '$base_n' did not match $phrase in the taxonomy table");
	}
    }
    
    return \@result if $return_type eq 'listref';
    return @result; # otherwise
}


sub lex_namestring {
    
    my ($taxonomy, $source_string) = @_;
    
    my (%prefixes, @names);
    
  LEXEME:
    while ( $source_string )
    {
	# Take out whitespace and commas at the beginning of the string (we
	# ignore these).
	
	if ( $source_string =~ qr{ ^ [\s,]+ (.*) }xs )
	{
	    $source_string = $1;
	    next LEXEME;
	}
	
	# Otherwise, grab everything up to the first comma.  This will be
	# taken to represent a taxonomic name possibly followed by exclusions.
	
	if ( $source_string =~ qr{ ^ ( [^,]+ ) (.*) }xs )
	{
	    $source_string = $2;
	    my $name_group = $1;
	    my $main_name = '';
	    
	    # From this string, take everything up to the first ^.  That's the
	    # main name.  Remove any whitespace at the end.
	    
	    if ( $name_group =~ qr{ ^ ( [^^]+ ) (.*) }xs )
	    {
		$name_group = $2;
		$main_name = $1;
		$main_name =~ s/\s+$//;
		
		# If the main name contains any invalid characters, just abort
		# the whole name group.
		
		if ( $main_name =~ qr{ [^a-zA-Z\s()%._:-] }xs )
		{
		    $taxonomy->add_warning('W_BAD_NAME', "Taxon name '$main_name' contains one or more invalid characters.");
		    next LEXEME;
		}
		
		# If the name includes a ':', split off the first component as a Selector prefix.
		# This will be looked up first, and used to resolve any ambiguities in the
		# remaining part of the name.  Repeat until there are no such prefixes left.
		
		my $prefix = '';
		
		while ( $main_name =~ qr{ ( [^:]+ ) [:] \s* (.*) }xs )
		{
		    $main_name = $2;
		    my $selector = $1;
		    
		    # Selector prefixes must consist of Roman letters only.  But they may have
		    # trailing whitespace and/or trailing wildcards, which is ignored.
		    
		    $selector =~ s/\s+$//g;
		    $selector =~ s/[%_.]+$//;
		    
		    if ( $selector =~ qr{ [^a-zA-Z] }xs || length($selector) < 4 )
		    {
			$taxonomy->add_warning('W_BAD_NAME', "Invalid selector '$selector', must " .
				"contain only Roman letters a-z and must have at least 4 letters.");
			next LEXEME;
		    }
		    
		    # Keep track of the prefix so far, because each prefix
		    # will need to be looked up before the main name is.
		    
		    $prefix .= "$selector:";
		    
		    $prefixes{$prefix} = 1;
		}
		
		# If there is still a : in the main name, then we have found a
		# syntax error.
		
		if ( $main_name =~ /:/ )
		{
		    $taxonomy->add_warning('W_BAD_NAME', "Taxon name '$main_name' does not match the pattern for a scientific name");
		    next LEXEME;
		}
		
		# In the main name, '.' should be taken as a wildcard that
		# ends a word (as in "T.rex").  So it should be translated
		# into '%' if it appears at the end of a name and '% '
		# otherwise.
		
		$main_name =~ s/[.](?:$|[:])/%/g;
		$main_name =~ s/[.]/% /g;
		
		# Condense any repeated wildcards and spaces.
		
		$main_name =~ s/%+/%/g;
		$main_name =~ s/\s+/ /g;
		
		# Now add the prefix(es) back to the main name.
		
		$main_name = $prefix . $main_name;
		
		# Add all of the prefxies collected so far to the name list,
		# and then clear the prefix hash.  We sort the prefixes
		# alphabetically, which means that shorter ones will always
		# come first.
		
		push @names, sort keys %prefixes;
		%prefixes = ();
		
		# Then add the name itself.  This won't end in ':', so it
		# counts as one of the actual names to resolve.
		
		push @names, $main_name;
	    }
	    
	    # Now, every successive string starting with '^' will represent an
	    # exclusion.  Remove any whitespace at the end.
	    
	EXCLUSION:
	    while ( $name_group =~ qr{ ^ \^+ \s* ( [^^]+ ) (.*) }xs )
	    {
		$name_group = $2;
		my $exclude_name = $1;
		$exclude_name =~ s/\s+$//;
		
		# If the exclusion contains any invalid characters, ignore it.
		
		if ( $exclude_name =~ qr{ [^\w\s%.:] }xs )
		{
		    $taxonomy->add_warning('W_BAD_NAME', "Taxon name '$exclude_name' contains invalid characters");
		    next EXCLUSION;
		}
		
		# If the name includes a ':', split off the first component as a Selector prefix.
		# This will be looked up first, and used to resolve any ambiguities in the
		# remaining part of the name.  Repeat until there are no such prefixes left.
		
		my $prefix = $main_name ? "$main_name:" : '';
		
		while ( $exclude_name =~ qr{ ( [^:]+ ) : [:\s]* (.*) }xs )
		{
		    $exclude_name = $2;
		    my $selector = $1;
		    
		    # Selector prefixes must consist of Roman letters only.  But they may have
		    # trailing whitespace and/or trailing wildcards, which is ignored.
		    
		    $selector =~ s/\s+$//g;
		    $selector =~ s/[%_.]+$//;
		    
		    if ( $selector =~ qr{ [^a-zA-Z] }xs || length($selector) < 4 )
		    {
			$taxonomy->add_warning('W_BAD_NAME', "Invalid selector '$selector', must " .
				"contain only Roman letters a-z and must have at least 4 letters.");
			next EXCLUSION;
		    }
		    
		    # Keep track of the prefix so far, because each prefix
		    # will need to be looked up before the main name is.
		    
		    $prefix .= "$selector:";
		    
		    $prefixes{$prefix} = 1;
		}
		
		# Any '.' should be taken as a wildcard with a space
		# following.  
		
		$exclude_name =~ s/[.]$/%/g;
		$exclude_name =~ s/[.]/% /g;
		
		# Condense any repeated wildcards and spaces.
		
		$exclude_name =~ s/%+/%/g;
		$exclude_name =~ s/\s+/ /g;
		
		# Add all of the prefxies collected for this exclusion to the
		# name list, and then clear the prefix hash.  We sort the
		# prefixes alphabetically, which means that shorter ones will
		# always come first.
		
		push @names, sort keys %prefixes;
		%prefixes = ();
		
		# Add this exclusion to the list of names to be resolved,
		# including the '^' flag and base name at the beginning.
		
		$exclude_name = "$prefix$exclude_name";
		
		push @names, "^$exclude_name";
	    }
	    
	    if ( $name_group ne '' )
	    {
		$taxonomy->add_warning('W_BAD_NAME', "Invalid exclusion '$name_group'");
	    }
	    
	    next LEXEME;
	}
	
	# If we get here, something went wrong with the parsing.
	
	else
	{
	    $taxonomy->add_warning('W_BAD_NAME', "Invalid taxon name '$source_string'");
	}
    }
    
    return @names;
}


sub lookup_base {
    
    my ($taxonomy, $base_name, $prefix_base, $options) = @_;
    
    return unless $base_name;
    
    my $dbh = $taxonomy->{dbh};
    
    # Names must contain only word characters, spaces, wildcards and dashes.
    
    unless ( $base_name =~ qr{ ^ ( \w [\w% -]+ ) $ }xs )
    {
	$taxonomy->add_warning('W_BAD_NAME', "invalid taxon name '$base_name'");
	return;
    }
    
    # If we were given a prefix base, construct a range clause.
    
    my $range_clause = '';
    
    if ( ref $prefix_base && $prefix_base->{lft} > 0 && $prefix_base->{rgt} )
    {
	$range_clause = 'and t.lft between '. $prefix_base->{lft} . ' and ' . $prefix_base->{rgt};
    }
    
    elsif ( defined $prefix_base && $prefix_base =~ qr{ ^ t.lft }xs )
    {
	$range_clause = "and ($prefix_base)";
    }
    
    # Count the number of Roman letters (not punctuation or spaces) in the
    # name.  This uses a very obscure quirk of Perl syntax to evaluate the =~
    # in scalar but not boolean context.  Note that taxonomic names, by
    # definition, are required to be composed of Roman letters only, so we
    # need not consider other alphabets or diacritical marks.
    
    my $letter_count = () = $base_name =~ m/[a-zA-Z]/g;
    
    # We require at least 4 actual letters.
    
    if ( $letter_count < 4 )
    {
	$taxonomy->add_warning('W_BAD_NAME', "Selector '$base_name' must contain at least 4 letters");
	return;
    }
    
    # If the base name doesn't contain any wildcards, see if we can find an
    # exactly coresponding taxonomic name.  If we can find at least one, pick
    # the one with the most subtaxa and we're done.
    
    if ( $base_name !~ qr{[%_]} && $letter_count >= 3 )
    {
	my $quoted = $dbh->quote($base_name);
	
	# Note that we use 'like' so that that differences in case and accent
	# marks will be ignored (note: taxonomic names don't have any accent
	# marks anyway.)
	
	my $sql = "
		SELECT t.orig_no, t.lft, t.rgt, (taxon_rank + 0) as taxon_rank
		FROM taxon_search as s JOIN taxon_trees as t1 using (orig_no)
			JOIN taxon_trees as t on t.orig_no = t1.synonym_no
			JOIN taxon_attrs as v on v.orig_no = t.orig_no
		WHERE taxon_name like $quoted and taxon_rank > 5 $range_clause
		GROUP BY orig_no
		ORDER BY v.taxon_size desc LIMIT 1";
	
	if ( ref $options->{debug_out} eq 'CODE' )
	{
	    &{$options->{debug_out}}("$sql\n");
	}
	
	my $result = $dbh->selectrow_hashref($sql);
	
	# If we found something, then we're done.
	
	if ( $result )
	{
	    return $result;
	}
    }
    
    # Otherwise, look for prefix matches.
    
    my $quoted = $dbh->quote("$base_name%");
    
    my $sql = "
	SELECT t.lft, t.rgt
	FROM taxon_search as s JOIN taxon_trees as t1 using (orig_no)
		JOIN taxon_trees as t on t.orig_no = t1.synonym_no
	WHERE taxon_name like $quoted and taxon_rank > 5 $range_clause
	GROUP BY t.orig_no";
    
    if ( ref $options->{debug_out} eq 'CODE' )
    {
	&{$options->{debug_out}}("$sql\n");
    }
    
    my $ranges = $dbh->selectall_arrayref($sql);
    
    if ( ref $ranges ne 'ARRAY' || @$ranges == 0 )
    {
	$taxonomy->add_warning('W_BAD_NAME', "Selector '$base_name' does not match any higher taxon in the taxonomy table");
	return;
    }
    
    # If we find more than 50 matches, then the prefix is not specific enough.
    
    if ( @$ranges > 50 )
    {
	$taxonomy->add_warning('W_BAD_NAME', "Selector '$base_name' is not specific enough, you must add more letters");
	return;
    }
    
    my @check = grep { $_->[0] > 0 && $_->[1] > 0 } @$ranges; 
    
    my $range_string = join(' or ', map { "t.lft between $_->[0] and $_->[1]" } @check);
    
    return $range_string;
}


# generate_id_string ( base, ignore_exclude )
# 
# This routine is called internally by each query method.  It decodes the base argument that was
# passed to the query method, and returns a string of taxon identifiers.  If $ignore_exclude is
# true, then the 'exclude' flag is ignored if it occurs in the base argument.

sub generate_id_string {
    
    my ($taxonomy, $base, $ignore_exclude, $exact) = @_;
    
    my @ids;
    
    # If $base is a reference to a Taxon object, return its taxon_no value (if given) or its
    # orig_no value.  But ignore it if the exclude flag is set, unless $ignore_exclude is given.
    
    if ( ref $base eq 'PBDB::Taxon' )
    {
	my $base_no = $base->{taxon_no} || $base->{orig_no};
	
	if ( $base_no && $base_no =~ $VALID_TAXON_ID && ( $ignore_exclude || ! $base->{exclude} ) )
	{
	    push @ids, $1;
	}
    }
    
    # If $base is a reference to an external identifier object, then use is
    # taxon_no value.  If the type is 'txn', then this is supposed to
    # represent the current spelling of the taxonomic concept associated with
    # the taxon_no value.  So if the $exact flag is true (meaning that we are
    # being asked to query for the exact name) then look up the current
    # spelling and use that.  Otherwise, the type will be 'var', which
    # indicates the exact name identified by this taxon_no.  Or else we don't
    # care because the 'exact' flag was not given.  So we can simply return
    # this number, and let the calling routine decide what to do with it.
    
    elsif ( ref $base eq 'PBDB::ExtIdent' )
    {
	if ( $base->{type} eq 'txn' && $exact )
	{
	    my $dbh = $taxonomy->{dbh};
	    my $TREE_TABLE = $taxonomy->{TREE_TABLE};
	    my $AUTH_TABLE = $taxonomy->{AUTH_TABLE};
	    my $num = $base->{taxon_no} // '0';
	    
	    my ($taxon_no) = $dbh->selectrow_array("
		SELECT spelling_no FROM $TREE_TABLE as t JOIN $AUTH_TABLE as a using (orig_no)
		WHERE taxon_no in ($num)");
	    
	    push @ids, $taxon_no if $taxon_no;
	}
	
	elsif ( $base->{num} )
	{
	    push @ids, $base->{num};
	}
    }
    
    # If $base is a reference to a TaxonSet object, return a string consisting of all keys that
    # are valid taxon identifiers.
    
    elsif ( ref $base eq 'PBDB::TaxonSet' )
    {
	push @ids, grep { $_ } map { $1 if $_ =~ $VALID_TAXON_ID } keys %$base;
    }
    
    # If $base is a reference to an array, then check all of the elements.  Collect up all valid
    # taxon identifiers that are found, along with the identifiers from all Taxon objects.
    
    elsif ( ref $base eq 'ARRAY' )
    {
	my ($dbh, $TREE_TABLE, $AUTH_TABLE);
	
	foreach my $t ( @$base )
	{
	    if ( ref $t eq 'PBDB::Taxon' )
	    {
		my $base_no = $t->{taxon_no} || $t->{orig_no};
		push @ids, $1 if $base_no && $base_no =~ $VALID_TAXON_ID && 
			( $ignore_exclude || ! $t->{exclude} );
	    }
	    
	    elsif ( ref $t eq 'PBDB::ExtIdent' )
	    {
		if ( $t->{type} eq 'txn' && $exact )
		{
		    $dbh //= $taxonomy->{dbh};
		    $TREE_TABLE //= $taxonomy->{TREE_TABLE};
		    $AUTH_TABLE //= $taxonomy->{AUTH_TABLE};
		    my $num = $t->{taxon_no} // '0';
		    
		    my ($taxon_no) = $dbh->selectrow_array("
		SELECT spelling_no FROM $TREE_TABLE as t JOIN $AUTH_TABLE as a using (orig_no)
		WHERE taxon_no in ($num)");
		    
		    push @ids, $taxon_no if $taxon_no;
		}
		
		elsif ( $t->{num} )
		{
		    push @ids, $t->{num};
		}
	    }
    
	    elsif ( ! ref $t )
	    {
		push @ids, grep { $_ } map { $1 if $_ =~ $VALID_TAXON_ID } split(qr{\s*,\s*}, $t);
	    }
	    
	    else
	    {
		croak "taxonomy: invalid taxon identifier '$t'\n";
	    }
	}
    }
    
    # Any other kind of reference (i.e. a code reference) will generate an error.
    
    elsif ( ref $base )
    {
	croak "taxonomy: invalid taxon identifier '$base'\n";
    }
    
    elsif ( $base )
    {
	push @ids, grep { $_ } map { $1 if $_ =~ $VALID_TAXON_ID } split(qr{\s*,\s*}, $base);
    }
    
    # Now return the list of taxon identifiers, joined with commas.  If no identifiers were found,
    # return the empty string.
    
    return join(q{,}, @ids);
}



# generate_opinion_id_string ( base )
# 
# This routine is called internally by some of the query methods.  It decodes the base argument
# that was passed to the query method, and returns a string of opinion_no values joined by
# commas. This return value is suitable for use in an SQL "in" expression.

sub generate_opinion_id_string {
    
    my ($taxonomy, $base) = @_;
    
    my (@ids, $id_list);
    
    # First convert $base into a list.  A scalar gets split on commas.  A listref is unchanged.
    # Any other type of reference is converted into a single-item list.
    
    if ( ref $base eq 'ARRAY' )
    {
	$id_list = $base;
    }
    
    else
    {
	my @list = ref $base ? ($base) : split(qr{\s*,\s*}, $base);
	$id_list = \@list;
    }
    
    # Now go through the list and extract one or more numeric identifiers from each element.
    
    foreach my $r ( @$id_list )
    {
	if ( ref $r eq 'PBDB::ExtIdent' )
	{
	    croak "taxonomy: invalid identifier type '$r->{type}', must be type 'opn'"
		unless $r->{type} eq 'opn' || $r->{type} eq 'unk';
	    push @ids, $r->{num} if defined $r->{num} && $r->{num} =~ $NUMERIC_ID;
	}
	
	elsif ( ref $r eq 'PBDB::Opinion' )
	{
	    my $opinion_no = $base->{opinion_no};
	    push @ids, $opinion_no if defined $opinion_no && $opinion_no =~ $NUMERIC_ID;
	}
	
	elsif ( ref $base eq 'PBDB::OpinionSet' )
	{
	    push @ids, grep { defined $_ && $_ =~ $NUMERIC_ID } keys %$base;
	}
	
	elsif ( ref $r )
	{
	    croak "taxonomy: invalid opinion identifier '$r'\n";
	}
	
	else
	{
	    push @ids, $r if defined $r && $r =~ $NUMERIC_ID;
	}
    }
    
    # If no valid identifiers were found, use "-1" which will ensure that nothing is selected.
    # This is the right thing to do, since if this routine is called then clearly the intent was
    # to select only things that match a valid opinion identifier.
    
    push @ids, -1 unless @ids;
    
    # Now return the list of opinion identifiers, joined with commas.
    
    return join(q{,}, @ids);
}


# generate_ref_id_string ( base )
# 
# This routine is called internally by some of the query methods.  It decodes the base argument
# that was passed to the query method, and returns a string of reference_no values joined by
# commas. This return value is suitable for use in an SQL "in" expression.

sub generate_ref_id_string {
    
    my ($taxonomy, $base) = @_;
    
    my (@ids, $id_list);
    
    # First convert $base into a list.  A scalar gets split on commas.  A listref is unchanged.
    # Any other type of reference is converted into a single-item list.
    
    if ( ref $base eq 'ARRAY' )
    {
	$id_list = $base;
    }
    
    else
    {
	my @list = ref $base ? ($base) : split(qr{\s*,\s*}, $base);
	$id_list = \@list;
    }
    
    # Now go through the list and extract one or more numeric identifiers from each element.
    
    foreach my $r ( @$id_list )
    {
	if ( ref $r eq 'PBDB::ExtIdent' )
	{
	    croak "taxonomy: invalid identifier type '$r->{type}', must be type 'ref'"
		unless $r->{type} eq 'ref' || $r->{type} eq 'unk';
	    push @ids, $r->{num} if defined $r->{num} && $r->{num} =~ $NUMERIC_ID;
	}
	
	elsif ( ref $r eq 'PBDB::Reference' )
	{
	    my $ref_no = $base->{reference_no};
	    push @ids, $ref_no if defined $ref_no && $ref_no =~ $NUMERIC_ID;
	}
	
	elsif ( ref $base eq 'PBDB::ReferenceSet' )
	{
	    push @ids, grep { defined $_ && $_ =~ $NUMERIC_ID } keys %$base;
	}
	
	elsif ( ref $r )
	{
	    croak "taxonomy: invalid reference identifier '$r'\n";
	}
	
	else
	{
	    push @ids, $r if defined $r && $r =~ $NUMERIC_ID;
	}
    }
    
    # If no valid identifiers were found, use "-1" which will ensure that nothing is selected.
    # This is the right thing to do, since if this routine is called then clearly the intent was
    # to select only things that match a valid reference identifier.
    
    push @ids, -1 unless @ids;
    
    # Now return the list of reference identifiers, joined with commas.
    
    return join(q{,}, @ids);
}


# exclusion_filters ( base )
# 
# This routine is called internally by some of the query methods.  For operations that need to
# respect taxon exclusions, this routine will return a list of filters that will exclude all
# children of any taxa marked with the 'exclude' flag.

sub exclusion_filters {

    my ($taxonomy, $base) = @_;
    
    # The only way this can happen is for the $base argument to be either a Taxon object or an
    # array of one or more of them.
    
    return unless ref $base eq 'ARRAY' or ref $base eq 'PBDB::Taxon';
    
    my @filters;
    
    foreach my $t ( ref $base eq 'ARRAY' ? @$base : $base )
    {
	next unless ref $t eq 'PBDB::Taxon';
	next unless $t->{exclude};
	next unless $t->{lft} && $t->{rgt};
	
	push @filters, "t.lft not between $t->{lft} and $t->{rgt}";
    }
    
    return @filters;
}


# immediate_filters ( base_string )
# 
# This routine is called when the relationship is 'all_children' and the
# option 'immediate' was also given.  It generates a filter which will
# exclude all children of junior synonyms of the specified base taxa.

sub immediate_filters {
    
    my ($taxonomy, $id_string, $options) = @_;
    
    # First query for the sequence range of each synonym of any of the specified taxa.
    
    my $dbh = $taxonomy->{dbh};
    
    my $auth_table = $taxonomy->{AUTH_TABLE};
    my $tree_table = $taxonomy->{TREE_TABLE};
    
    my $sql = "SELECT t.lft, t.rgt, t.orig_no as junior_no, tb.orig_no as senior_no
	FROM $auth_table as base JOIN $tree_table as tb using (orig_no)
		LEFT JOIN $tree_table as t on t.synonym_no = tb.orig_no
	WHERE base.taxon_no in ($id_string)
	GROUP BY t.orig_no";
    
    if ( ref $options->{debug_out} eq 'CODE' )
    {
	&{$options->{debug_out}}("$sql\n");
    }
    
    my $rows = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    return unless ref $rows eq 'ARRAY' && @$rows;
    
    # Now comes the tricky part.  We have to add an exclusion for each
    # sequence range, EXCEPT for those taxa that were also specified as base
    # taxa. The reason for this is so that if the user specifies,
    # i.e. "stegosaurus,hesperosaurus" then they will get all of the immediate
    # children of those two taxa and not of any of the other synonyms of
    # stegosaurus.  This will require two passes through the list.
    
    my %base;
    my @exclusions;
    
    foreach my $r ( @$rows )
    {
	$base{$r->{senior_no}} = 1 if $r->{senior_no};
    }
    
    foreach my $r ( @$rows )
    {
	next if $base{$r->{junior_no}};
	next unless $r->{lft} && $r->{rgt};
	
	push @exclusions, "t.lft not between $r->{lft} and $r->{rgt}";
    }
    
    return @exclusions;
}


# generate_range_filter ( id_string )
# 
# This routine translates a string list of taxon identifiers into a filter
# that will restrict to a particular taxonomic range.

sub range_filter {

    my ($taxonomy, $id_string) = @_;
    
    # Return an empty result unless there actually is at least one taxon
    # identifier. 
    
    return unless $id_string;
    
    # Query the database for the specified taxa, asking specifically for the
    # taxonomic range(s).
    
    my $dbh = $taxonomy->{dbh};
    
    my $auth_table = $taxonomy->{AUTH_TABLE};
    my $tree_table = $taxonomy->{TREE_TABLE};
    
    my $sql = "	SELECT t.lft, t.rgt
		FROM $auth_table as a JOIN $tree_table as t using (orig_no)
		WHERE a.taxon_no in ($id_string)
		GROUP BY t.orig_no";
    
    my $rows = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    return unless ref $rows eq 'ARRAY' && @$rows;
    
    my @ranges = map { "t.lft between $_->{lft} and $_->{rgt}" } @$rows;
    my $range_string = join( ' or ', @ranges );
    
    return @ranges == 1 ? $range_string : "($range_string)";
}


# copy_exclusions ( result_list, base )
# 
# This routine is called internally by some of the query methods.  For operations that do not
# respect taxon exclusions, i.e. list_taxa_simple, any exclusion flags must be copied from the
# base argument to any corresponding results.
# 
# The reason for this is as follows.  Suppose that some client code generates a list of Taxon
# objects, some of which may have exclusions (i.e. by calling &resolve_names).  It may then wish
# to call &list_taxa_simple or &list_taxa to get more information about these objects, while
# keeping any exclusion flags in place.  This routine will be called internally to set those flags
# properly based on the 'base' argument.

sub copy_exclusions {
    
    my ($taxonomy, $result_list, $base) = @_;
    
    # The only way this can happen is for the $base argument to be either a Taxon object or an
    # array of one or more of them.
    
    return unless ref $base eq 'ARRAY' or ref $base eq 'PBDB::Taxon';
    
    my %exclude;
    
    # First process $base to detect any exclusions.
    
    foreach my $t ( ref $base eq 'ARRAY' ? @$base : $base )
    {
	$exclude{$t->{orig_no}} = 1 if ref $t eq 'PBDB::Taxon' && $t->{orig_no} && $t->{exclude};
    }
    
    # Then go through the result list and apply these exclusions.
    
    foreach my $t ( @$result_list )
    {
	$t->{exclude} = 1 if $exclude{$t->{orig_no}};
    }
    
    return;
}


sub generate_fields {
    
    my ($taxonomy, $fields, $tables_hash) = @_;
    
    my @field_list;
    
    if ( ref $fields eq 'ARRAY' )
    {
	@field_list = @$fields;
    }
    
    elsif ( ref $fields )
    {
	croak "taxonomy: bad field specifier '$fields'\n";
    }
    
    elsif ( defined $fields )
    {
	@field_list = split qr{\s*,\s*}, $fields;
    }
    
    my (@result, %uniq);
    
    foreach my $f ( @field_list )
    {
	next unless $f;
	croak "taxonomy: unknown field specifier '$f'\n" unless ref $FIELD_LIST{$f};
	
	$f = 'AUTH_SIMPLE' if $f eq 'SIMPLE' && $tables_hash->{use_a};
	$f = 'AUTH_DATA' if $f eq 'DATA' && $tables_hash->{use_a};
	$f = 'AUTH_SEARCH' if $f eq 'SEARCH' && $tables_hash->{use_a};
	
	foreach my $n ( @{$FIELD_LIST{$f}} )
	{
	    next if $uniq{$n};
	    $uniq{$n} = 1;
	    push @result, $n;
	}
	
	# Note that the following shortcut implies at most five different
	# tables for any particular field specifier.  This can be changed if
	# necessary.
	
	@{$tables_hash}{@{$FIELD_TABLES{$f}}} = (1, 1, 1, 1, 1) if ref $FIELD_TABLES{$f};
    }
    
    croak "taxonomy: no valid fields specified\n" unless @result;
    
    return @result;
}



our (%STATUS_FILTER) = ( valid => "t.accepted_no = t.synonym_no",
			accepted => "t.accepted_no = t.orig_no",
			senior => "t.accepted_no = t.orig_no",
			junior => "t.accepted_no = t.synonym_no and t.orig_no <> t.synonym_no",
			invalid => "t.accepted_no <> t.synonym_no",
		        any => '1=1',
		        all => '1=1');

# our (%VARIANT_FILTER) = ( valid => 1, accepted => 1, senior => 1, junior => 1 );

our (%OP_TYPE_FILTER) = ( valid => "in ('belongs to', 'subjective synonym of', 'objective synonym of', 'replaced by')",
			 accepted => "in ('belongs to')",
			 senior => "in ('belongs to')",
			 junior => "in ('subjective synonym of', 'objective synonym of', 'replaced by')",
			 invalid => "not in ('belongs to', 'subjective synonym of', 'objective synonym of', 'replaced by')",
			 any => 'is not null',
			 all => 'is not null',
			 opinions => 'is not null',
			 class => 'is not null');

sub taxon_filters {
    
    my ($taxonomy, $options, $tables_ref) = @_;
    
    my @filters;
    
    # If the 'status' option was given, add the appropriate filter.
    
    if ( $options->{status} )
    {
	my $filter = $STATUS_FILTER{$options->{status}};
	
	if ( defined $filter )
	{
	    push @filters, $filter unless $filter eq '1=1';
	}
	
	else
	{
	    croak "bad value '$options->{status}' for option 'status'\n";
	}
	
	# # If the status was given as 'valid', 'accepted', 'senior' or 'junior', then also filter
	# # out all name variant other than the current one unless either (a) we are fetching
	# # opinions, or (b) the 'use_a' flag is set.
	
	# if ( $VARIANT_FILTER{$options->{status}} && ! $options->{all_variants} )
	# {
	#     push @filters, "a.taxon_no = t.spelling_no";
	# }
    }
    
    if ( $options->{min_rank} || $options->{max_rank} )
    {
	my $min = $options->{min_rank} > 0 ? $options->{min_rank} + 0 : $TAXON_RANK{lc $options->{min_rank}};
	my $max = $options->{max_rank} > 0 ? $options->{max_rank} + 0 : $TAXON_RANK{lc $options->{max_rank}};
	
	if ( $min && $max )
	{
	    push @filters, $min == $max ? "t.rank = $min" : "t.rank between $min and $max";
	}
	
	elsif ( $min )
	{
	    push @filters, "t.rank >= $min";
	}
	
	elsif ( $max )
	{
	    push @filters, "t.rank <= $max";
	}
	
	else
	{
	    if ( $options->{min_rank} && ! $min )
	    {
		croak "bad value '$options->{min_rank}' for option 'min_rank'\n";
	    }
	    
	    else
	    {
		croak "bad value '$options->{max_rank}' for option 'max_rank'\n";
	    }
	}
    }
    
    if ( $options->{rank} )
    {
	my @selectors = ref $options->{rank} eq 'ARRAY' ? @{$options->{rank}}
	    : split qr{\s*,\s*}, $options->{rank};
	
	my (@rank_filters, @ranks);
	
	foreach my $s (@selectors)
	{
	    next unless defined $s && $s ne '';
	    
	    if ( $s =~ qr{ ^ (.+?) - (.+) }xs )
	    {
		my $bottom = $1;
		my $top = $2;
		
		my $expr1 = $taxonomy->rank_filter($bottom, 'bottom', $tables_ref);
		my $expr2 = $taxonomy->rank_filter($top, 'top', $tables_ref);
		
		push @rank_filters, "($expr1 and $expr2)";
	    }
	    
	    elsif ( $RANK_STRING{$s} )
	    {
		push @ranks, $s;
	    }
	    
	    else
	    {
		push @rank_filters, $taxonomy->rank_filter($s, 'single', $tables_ref);
	    }
	}
	
	if ( @ranks )
	{
	    my $rank_string = join( q{,}, @ranks );
	    
	    if ( $tables_ref->{use_a} )
	    {
		push @rank_filters, "(a.taxon_rank + 0) in ($rank_string)";
	    }
	    
	    else
	    {
		push @rank_filters, "t.rank in ($rank_string)";
	    }
	}
	
	my $rank_filter;
	
	if ( @rank_filters == 0 )
	{
	    $rank_filter = 't.rank = 0';
	}
	
	elsif ( @rank_filters == 1 )
	{
	    $rank_filter = $rank_filters[0];
	}
	
	else
	{
	    $rank_filter = '(' . join( q{ or }, @rank_filters ) . ')';
	}
	
	# Check for errors
	
	croak "bad rank specification\n" if $rank_filter =~ qr{ERROR};
	
	# Add the specified filter.
	
	push @filters, $rank_filter;
    }
    
    if ( ref $options->{pres} eq 'HASH' && ! $options->{pres}{all} )
    {
	my @keys = keys %{$options->{pres}};
	
	if ( @keys == 1 )
	{
	    $tables_ref->{v} = 1;
	    
	    if ( $keys[0] eq 'regular' )
	    {
		push @filters, "not(v.is_trace or v.is_form)";
	    }
	    
	    elsif ( $keys[0] eq 'form' )
	    {
		push @filters, "v.is_form";
	    }
	    
	    elsif ( $keys[0] eq 'ichno' )
	    {
		push @filters, "v.is_trace";
	    }
	    
	    else
	    {
		push @filters, "a.taxon_no = -1";
		croak "bad value '$keys[0]' for option 'pres'\n";
	    }
	}
	
	elsif ( @keys == 2 )
	{
	    $tables_ref->{v} = 1;
	    
	    if ( $options->{pres}{form} && $options->{pres}{ichno} )
	    {
		push @filters, "(v.is_trace or v.is_form)";
	    }
	    
	    elsif ( $options->{pres}{form} && $options->{pres}{regular} )
	    {
		push @filters, "(v.is_form or not(v.is_trace))";
	    }
	    
	    elsif ( $options->{pres}{ichno} && $options->{pres}{regular} )
	    {
		push @filters, "(v.is_trace or not(v.is_form))";
	    }
	    
	    else
	    {
		croak "bad value '$keys[0]', '$keys[1]' for option 'pres'\n";
	    }
	}
	
	else
	{
	    unless ( $options->{pres}{form} && $options->{pres}{ichno} && $options->{pres}{regular} )
	    {
		croak "bad value '$keys[0]', '$keys[1]', '$keys[2]' for option 'pres'\n";
	    }
	    
	    # No filter to add in this case, since all classes of taxa are
	    # selected.
	}
    }
    
    elsif ( defined $options->{pres} )
    {
	croak "bad value '$options->{pres}' for option 'pres': must be a hashref\n";
    }
    
    if ( defined $options->{extant} && $options->{extant} ne '' )
    {
	$tables_ref->{v} = 1;
	
	if ( $options->{extant} )
	{
	    push @filters, "v.is_extant = 1";
	}
	
	else
	{
	    push @filters, "v.is_extant = 0";
	}
    }
    
    if ( defined $options->{depth} && $options->{depth} ne '' )
    {
	if ( $options->{depth} > 0 || $options->{depth} eq '0' )
	{
	    my $max_depth = $options->{depth} + 0;
	
	    push @filters, "(t.depth - tb.depth) <= $max_depth";
	}
	
	else
	{
	    croak "bad value '$options->{depth}' for option 'depth'\n";
	}
    }
    
    if ( $options->{max_ma} || $options->{min_ma} )
    {
	$tables_ref->{v} = 1;
	
	if ( $options->{max_ma} =~ qr{^ (?: [0-9][0-9.]* | \.[0-9][0-9]* ) $}xs )
	{
	    my $max_ma = $options->{max_ma} + 0;
	    
	    push @filters, "v.last_late_age < $max_ma";
	}
	
	elsif ( defined $options->{max_ma} && $options->{max_ma} ne '' )
	{
	    croak "bad value '$options->{max_ma}' for option 'max_ma'\n";
	}
	
	if ( $options->{min_ma} =~ qr{^ (?: [0-9][0-9.]* | \.[0-9][0-9]* ) $}xs )
	{
	    my $min_ma = $options->{min_ma} + 0;
	    
	    push @filters, "v.first_early_age > $min_ma";
	}
	
	elsif ( defined $options->{min_ma} && $options->{min_ma} ne '' )
	{
	    croak "bad value '$options->{min_ma}' for option 'min_ma'\n";
	}	
    }
    
    if ( $options->{min_pubyr} || $options->{max_pubyr} )
    {
	$tables_ref->{v} = 1;
	
	if ( $options->{min_pubyr} =~ qr{ ^ [0-9]+ $ }xs )
	{
	    push @filters, "v.pubyr >= '$options->{min_pubyr}'";
	}
	
	elsif ( defined $options->{min_pubyr} && $options->{min_pubyr} ne '' )
	{
	    croak "bad value '$options->{min_pubyr}' for option 'min_pubyr'\n";
	}
	
	if ( $options->{max_pubyr} =~ qr{ ^ [0-9]+ $ }xs )
	{
	    push @filters, "v.pubyr <= '$options->{max_pubyr}'";
	}
	
	elsif ( defined $options->{max_pubyr} && $options->{max_pubyr} ne '' )
	{
	    croak "bad value '$options->{max_pubyr}' for option 'max_pubyr'\n";
	}
    }
    
    push @filters, $taxonomy->common_filters('a', 'taxa', $options);
    
    return @filters;
}


my %RANK_CMP = ( max => '<=', below => '<', min => '>=', above => '>' );
my %RANK_FIELD = ( max => 'max_rank', below => 'max_rank', 
		   min => 'min_rank', above => 'min_rank' );

sub rank_filter {
    
    my ($taxonomy, $rank, $type, $tables_ref) = @_;
    
    my $prefix = '';
    
    if ( $RANK_STRING{$rank} )
    {
	if ( $type eq 'single' )
	{
	    if ( $tables_ref->{use_a} )
	    {
		return "(a.taxon_rank + 0) = $rank";
	    }
	    
	    else
	    {
		return "t.rank = $rank";
	    }
	}
	
	else
	{
	    $prefix = $type eq 'bottom' ? 'min' : 'max';
	}
    }
    
    elsif ( $rank =~ qr{ ^ ( above | below | min | max ) _ (.+) }xsi )
    {
	$prefix = $1;
	$rank = $2;
	
	return "ERROR" unless $RANK_STRING{$rank};
    }
    
    else
    {
	return "ERROR";
    }
    
    # Now construct the filter.
    
    if ( $tables_ref->{use_a} )
    {
	return "(a.taxon_rank + 0) $RANK_CMP{$prefix} $rank";
    }
    
    else
    {
	return "t.$RANK_FIELD{$prefix} $RANK_CMP{$prefix} $rank";
    }

    # if ( $prefix eq 'max' )
    # {
    # 	return "t.max_rank <= $rank";
    # }
    
    # elsif ( $prefix eq 'below' )
    # {
    # 	return "t.max_rank < $rank";
    # }
    
    # elsif ( $prefix eq 'min' )
    # {
    # 	return "t.min_rank >= $rank";
    # }
    
    # else # ( $prefix eq 'above' )
    # {
    # 	return "t.min_rank > $rank";
    # }
    
}


sub ref_filters {
    
    my ($taxonomy, $options, $tables_ref) = @_;
    
    my @filters;
    my $dbh = $taxonomy->{dbh};
    
    if ( $options->{language} )
    {
	my $language = $dbh->quote($options->{language});
	push @filters, "r.language = $language";
    }
    
    if ( $options->{min_pubyr} || $options->{max_pubyr} )
    {
	my $min = $dbh->quote($options->{min_pubyr}) if $options->{min_pubyr};
	my $max = $dbh->quote($options->{max_pubyr}) if $options->{max_pubyr};
	
	if ( $min && $max )
	{
	    push @filters, $min eq $max ? "r.pubyr = $min" : "r.pubyr between $min and $max";
	}
	
	elsif ( $min )
	{
	    push @filters, "r.pubyr >= $min";
	}
	
	elsif ( $max )
	{
	    push @filters, "r.pubyr <= $max";
	}
	
	else
	{
	    push @filters, "r.pubyr = '-1'";
	}
    }
    
    if ( $options->{ref_author} )
    {
	if ( $options->{ref_author} =~ /(.*?) \s+ and \s+ (.*)/xs )
	{
	    my $a1 = $1; my $a2 = $2;
	    
	    my $q1 = $dbh->quote($a1);
	    my $q2 = $dbh->quote($a2);
	    my $q3 = $dbh->quote("\\b$a2\\b");
	    
	    push @filters, "r.author1last like $q1 and (r.author2last like $q2 or r.otherauthors rlike $q3)";
	}
	
	elsif ( $options->{ref_author} =~ /(.*?) \s+ et al[.]?/xs )
	{
	    my $a1 = $1;
	    my $q1 = $dbh->quote($a1);
	    
	    push @filters, "r.author1last like $q1";
	}
	
	else
	{
	    my $q1 = $dbh->quote($options->{author});
	    my $q2 = $dbh->quote("\\b" . $options->{author} . "\\b"); 
	    
	    push @filters, "(r.author1last like $q1 or r.author2last like $q1 or r.otherauthors rlike $q2)";
	}
    }
    
    if ( $options->{pub_title} )
    {
	my $pubtitle = $dbh->quote($options->{pubtitle});
	
	push @filters, "r.pubtitle like $pubtitle";
    }
    
    push @filters, $taxonomy->common_filters('r', 'refs', $options);
    
    $tables_ref->{r} = 1 if @filters;
    
    return @filters;
}


sub opinion_filters {
    
    my ($taxonomy, $options, $tables_ref) = @_;
    
    my @filters;
    my $dbh = $taxonomy->{dbh};
    
    my @stuff = caller;
    my $caller = $stuff[3];
    # print STDERR "caller: $caller\n";
    
    if ( defined $options->{opinion_no} && $options->{opinion_no} ne 'all_records' )
    {
	my $opinion_ids = $taxonomy->generate_opinion_id_string($options->{opinion_no}) || '-1';
	push @filters, "o.opinion_no in ($opinion_ids)" if $opinion_ids;
    }
    
    if ( $options->{op_type} )
    {
	my $filter = $OP_TYPE_FILTER{$options->{op_type}};
	
	if ( defined $filter )
	{
	    push @filters, "o.status $filter" unless $filter eq 'is not null';
	}
	
	else
	{
	    push @filters, "o.status = 'NOTHING'";
	}
    }
    
    if ( $options->{op_min_pubyr} || $options->{op_max_pubyr} )
    {
	my $min = $dbh->quote($options->{op_min_pubyr}) if $options->{op_min_pubyr};
	my $max = $dbh->quote($options->{op_max_pubyr}) if $options->{op_max_pubyr};
	
	if ( $min && $max )
	{
	    push @filters, $min eq $max ? "o.pubyr = $min" : "o.pubyr between $min and $max";
	}
	
	elsif ( $min )
	{
	    push @filters, "o.pubyr >= $min";
	}
	
	elsif ( $max )
	{
	    push @filters, "o.pubyr <= $max";
	}
	
	else
	{
	    push @filters, "o.pubyr = '-1'";
	}
    }
    
    if ( $options->{op_author} )
    {
	my (@authors) = ref $options->{op_author} eq 'ARRAY' ? @{$options->{op_author}} : '';
	
	my @author_filters;
	
	foreach my $a ( @authors )
	{
	    next unless $a && $a =~ /\w/;
	    
	    if ( $a =~ / and | et al[.]/s )
	    {
		my $quoted = $dbh->quote($a);
		push @author_filters, "o.author like $quoted";
	    }
	    
	    else
	    {
		$a =~ s/%/.*/g;
		$a =~ s/_/./g;
		my $quoted = $dbh->quote("^$a\$|^$a and |^$a et al| and $a\$");
		push @author_filters, "o.author rlike $quoted";
	    }
	}
	
	push @author_filters, "o.author = 'SELECT_NONE'" unless @author_filters;
	
	push @filters, '(' . join(' or ', @author_filters) . ')';
    }
    
    push @filters, $taxonomy->common_filters('oo', 'ops', $options);
    
    return @filters;
}


sub common_filters {

    my ($taxonomy, $t, $opt_type, $options, $tables) = @_;
    
    my @filters;
    my $dbh = $taxonomy->{dbh};
    
    # Go through all of the options passed to this Taxonomy request.
    
    foreach my $key ( keys %$options )
    {
	# Check to see if any of them is a common option which matches the
	# given $opt_type.
	
	next unless $COMMON_OPTION{$key};
	
	my $value = $options->{$key};
	
	next unless defined $value && $value ne '';
	
	if ( $key =~ qr{^ref} )
	{
	    next unless $opt_type eq 'refs';
	}
	
	elsif ( $key =~ qr{^op} )
	{
	    next unless $opt_type eq 'ops';
	}
	
	else
	{
	    next unless $opt_type eq 'taxa';
	}
	
	# If so, then create the appropriate SQL filter expression and add it
	# to the list.
	
	if ( $key =~ qr{created_after$} )
	{
	    my $date = $dbh->quote($value);
	    push @filters, "$t.created >= $date";
	}
	
	elsif ( $key =~ qr{created_before$} )
	{
	    my $date = $dbh->quote($value);
	    push @filters, "$t.created < $date";
	}
	
	elsif ( $key =~ qr{modified_after$} )
	{
	    my $date = $dbh->quote($value);
	    push @filters, "$t.modified >= $date";
	}
	
	elsif ( $key =~ qr{modified_before$} )
	{
	    my $date = $dbh->quote($value);
	    push @filters, "$t.modified < $date";
	}
	
	elsif ( $key =~ qr{authorized_by$} )
	{
	    push @filters, $taxonomy->person_id_filter($t, 'authorizer_no', $value);
	}
	
	elsif ( $key =~ qr{entered_by$} )
	{
	    push @filters, $taxonomy->person_id_filter($t, 'enterer_no', $value);
	    
	    # my ($list, $exclude) = $taxonomy->person_id_list($value);
	    # if ( $list eq '-9998' ) {
	    # 	push @filters, "$t.authorizer_no <> $t.enterer_no";
	    # } elsif ( $list eq '-9997' ) {
	    # 	push @filters, "$t.authorizer_no = $t.enterer_no";
	    # } elsif ( $list ne '-9999' ) {
	    # 	push @filters, "$t.enterer_no ${exclude}in ($list)";
	    # }
	}
	
	elsif ( $key =~ qr{authent_by$} )
	{
	    push @filters, $taxonomy->person_id_filter($t, 'authent', $value);
	    
	    # my ($list, $exclude) = $taxonomy->person_id_list($value);
	    # if ( $list eq '-9998' ) {
	    # 	push @filters, "$t.authorizer_no <> $t.enterer_no";
	    # } elsif ( $list eq '-9997' ) {
	    # 	push @filters, "$t.authorizer_no = $t.enterer_no";
	    # } elsif ( $list ne '-9999' ) {
	    # 	push @filters, "${exclude}($t.authorizer_no in ($list) or $t.enterer_no in ($list))";
	    # }
	}
	
	elsif ( $key =~ qr{modified_by$} )
	{
	    push @filters, $taxonomy->person_id_filter($t, 'modifier_no', $value);
	    
	    # my ($list, $exclude) = $taxonomy->person_id_list($value);
	    # if ( $list eq '-9998' ) {
	    # 	push @filters, "$t.modifier_no <> $t.enterer_no and $t.modifier_no <> 0";
	    # } elsif ( $list eq '-9997' ) {
	    # 	push @filters, "$t.modifier_no = $t.enterer_no";
	    # } elsif ( $list eq '0' ) {
	    # 	push @filters, "$t.modifier_no ${exclude}in ($list)";
	    # } elsif ( $list eq '-9999' ) {
	    # 	push @filters, "$t.modifier_no <> 0";
	    # } else {
	    # 	push @filters, "$t.modifier_no ${exclude}in ($list) and $t.modifier_no <> 0";
	    # }
	}
	
	elsif ( $key =~ qr{touched_by$} )
	{
	    push @filters, $taxonomy->person_id_filter($t, 'touched', $value);
	    
	    # my ($list, $exclude) = $taxonomy->person_id_list($value);
	    # if ( $list !~ /^-/ ) {
	    # 	push @filters, 
	    # 	"$exclude($t.authorizer_no in ($list) or $t.enterer_no in ($list) or $t.modifier_no in ($list))";
	    # } else {
	    # 	push @filters, "1=1";
	    # }
	}
	
	# elsif ( $key =~ qr{min_pubyr$|max_pubyr$} )
	# {
	#     my $comp = $key =~ qr{min} ? '>=' : '<=';
	#     my $year = $dbh->quote($value);
	    
	#     if ( $opt_type eq 'taxa' )
	#     {
	# 	push @filters, "v.pubyr $comp $year";
	# 	$tables->{v} = 1;
	#     }
	    
	#     elsif ( $opt_type eq 'op' )
	#     {
	# 	push @filters, "o.pubyr $comp $year";
	#     }
	    
	#     elsif ( $opt_type eq 'ref' )
	#     {
	# 	push @filters, "r.pubyr $comp $year";
	#     }
	# }
	
	else
	{
	    croak "taxonomy: bad common option '$key'\n";
	}
    }
    
    # Now return the list of filters (possibly empty).
    
    return @filters;
}


sub person_id_filter {
    
    my ($taxonomy, $tn, $param, $value) = @_;
    
    return "$tn.authorizer_no = -1" unless ref $value eq 'ARRAY' && defined $value->[0];
    
    my $output = join(',', @$value);
    # print STDERR "id_list: $output\n";
    
    my $exclude;
    my $any_except;
    
    if ( $value->[0] eq '_EXCLUDE_' )
    {
	shift @$value;
	$exclude = 1;
    }
    
    elsif ( $value->[0] eq '_ANY_EXCEPT_' )
    {
	shift @$value;
	$any_except = 1;
    }
    
    elsif ( $value->[0] eq '_ANY_' )
    {
	if ( $param eq 'modifier_no' or $param eq 'enterer_no' or $param eq 'authorizer_no' )
	{
	    return "$tn.modifier_no <> 0";
	}
	
	else
	{
	    return "1=1";
	}
    }
    
    elsif ( $value->[0] eq '_SAME_' || $value->[0] eq '_DIFFERENT_' )
    {
	my $op = $value->[0] eq '_SAME_' ? '=' : '<>';
	
	if ( $param eq 'authorizer_no' or $param eq 'enterer_no' or $param eq 'authent' )
	{
	    return "$tn.authorizer_no $op $tn.enterer_no";
	}
	
	# elsif ( $param eq 'modifier_no' )
	# {
	#     my $ex = $value->[0] eq '_DIFFERENT_' ? " and $tn.modifier_no <> 0" : '';
	#     return "$tn.modifier_no $op $tn.enterer_no$ex";
	# }
	
	elsif ( $param eq 'modifier_no' )
	{
	    if ( $value->[0] eq '_SAME_' )
	    {
		return "($tn.modifier_no = $tn.enterer_no or $tn.modifier_no = $tn.authorizer_no" .
		    " or $tn.modifier_no = 0)";
	    }
	    
	    else
	    {
		return "$tn.modifier_no <> $tn.enterer_no and $tn.modifier_no <> $tn.authorizer_no" .
		    " and $tn.modifier_no <> 0"
	    }
	}
	
	else
	{
	    return "1=1";
	}
    }
    
    # If we get here then we have a list of ids to work with.
    
    my $id_list = join(',', @$value);
    
    my $op = $exclude || $any_except ? 'not ' : '';
    
    if ( $param eq 'touched' )
    {
	return "$op($tn.authorizer_no in ($id_list) or $tn.enterer_no in ($id_list) or $tn.modifier_no in ($id_list))";
    }
    
    elsif ( $param eq 'authent' )
    {
	return "$op($tn.authorizer_no in ($id_list) or $tn.enterer_no in ($id_list))";
    }
    
    else
    {
	my $ex = $any_except ? " and $tn.$param <> 0" : '';
	return "$tn.$param ${op}in ($id_list)$ex";
    }
}


# refno_filter ( options, table )
# 
# If the option 'reference_no' was given, then return a filter expression that will select only
# records for which the reference_no value in the specified table matches one of the specified
# reference_no values.  Otherwise, return nothing.

sub refno_filter {

    my ($taxonomy, $options, $table) = @_;
    
    if ( $options->{reference_no} )
    {
	my $ref_string = $taxonomy->generate_ref_id_string($options->{reference_no});
	return "$table.reference_no in ($ref_string)";
    }
    
    else
    {
	return;
    }
    
}


sub extra_filters {

    my ($taxonomy, $options) = @_;
    
    return unless ref $options->{extra_filters} eq 'ARRAY' && @{$options->{extra_filters}};
    
    return @{$options->{extra_filters}};
}


my $IDENT_UNCERTAIN = "'aff.', 'cf.', '?', '\"', 'sensu lato', 'informal'";

sub occ_filters {
    
    my ($taxonomy, $options, $tn) = @_;
    
    my @filters;
    
    if ( ! $options->{ident_select} || $options->{ident_select} eq 'latest' )
    {
	push @filters, "$tn.latest_ident = true";
    }
    
    elsif ( $options->{ident_select} eq 'orig' )
    {
	push @filters, "$tn.reid_no = 0";
    }
    
    elsif ( $options->{ident_select} eq 'reid' )
    {
	push @filters, "($tn.reid_no > 0 or ($tn.reid_no = 0 and $tn.latest_ident = false))";
    }
    
    else # ident_select eq 'all'
    {
	# no filter needed
    }
    
    if ( my $idqual = $options->{ident_qual} )
    {
	if ( $idqual eq 'certain' || $idqual eq 'genus_certain' )
	{
	    push @filters, "$tn.genus_reso not in ($IDENT_UNCERTAIN)";
	    push @filters, "$tn.subgenus_reso not in ($IDENT_UNCERTAIN)";
	    push @filters, "$tn.species_reso not in ($IDENT_UNCERTAIN)" if $idqual eq 'certain';
	}
	
	elsif ( $idqual eq 'uncertain' )
	{
	    push @filters, "($tn.genus_reso in ($IDENT_UNCERTAIN) or " .
		"$tn.subgenus_reso in ($IDENT_UNCERTAIN) or $tn.species_reso in ($IDENT_UNCERTAIN))";
	}
	
	elsif ( $idqual eq 'new' )
	{
	    push @filters, "($tn.genus_reso = 'n. gen.' or $tn.subgenus_reso = 'n. subgen.' or $tn.species_reso = 'n. sp.')";
	}
	
	else # idqual eq 'any'
	{
	    # no filter needed
	}
    }
    
    return @filters;
}


sub taxon_order {
    
    my ($taxonomy, $rel, $type, $options, $tables_ref) = @_;
    
    return '' unless $options->{order};
    
    my (@elements, @clauses);
    
    if ( ref $options->{order} eq 'ARRAY' )
    {
	@elements = @{$options->{order}};
    }
    
    else
    {
	@elements = split qr{\s*,\s*}, $options->{order};
    }
    
    foreach my $order (@elements)
    {
	$order = lc $order;
	
	if ( $order eq 'name' or $order eq 'name.asc' )
	{
	    push @clauses, $tables_ref->{use_a} ? "a.taxon_name asc" : "t.name asc";
	}
	
	elsif ( $order eq 'name.desc' )
	{
	    push @clauses, $tables_ref->{use_a} ? "a.taxon_name desc" : "t.name desc";
	}
	
	elsif ( $order eq 'hierarchy' || $order eq 'hierarchy.asc' )
	{
	    if ( $rel eq 'children' || $rel eq 'all_children' || $rel eq 'parents' ||
		 $rel eq 'all_parents' || $rel eq 'synonyms' )
	    {
		push @clauses, "t.lft asc";
	    }
	    
	    else
	    {
		push @clauses, "if(t.lft > 0, 0, 1), t.lft asc";
	    }	    
	}
	
	elsif ( $order eq 'hierarchy.desc' )
	{
	    if ( $rel eq 'children' || $rel eq 'all_children' || $rel eq 'parents' ||
		 $rel eq 'all_parents' || $rel eq 'synonyms' )
	    {
		push @clauses, "t.lft desc";
	    }
	    
	    else
	    {
		push @clauses, "if(t.lft > 0, 1, 0), t.lft desc";
	    }	    
	}
	
	elsif ( $order eq 'n_occs' or $order eq 'n_occs.desc' )
	{
	    push @clauses, "v.n_occs desc";
	    $tables_ref->{v} = 1;
	}

	elsif ( $order eq 'n_occs.asc' )
	{
	    push @clauses, "v.n_occs asc";
	    $tables_ref->{v} = 1;
	}

	elsif ( $order eq 'size' or $order eq 'size.desc' )
	{
	    push @clauses, "v.taxon_size desc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'size.asc' )
	{
	    push @clauses, "v.taxon_size asc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'extsize' or $order eq 'extsize.desc' )
	{
	    push @clauses, "v.extant_size desc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'extsize.asc' )
	{
	    push @clauses, "v.extant_size asc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'extant' or $order eq 'extant.desc' )
	{
	    push @clauses, "v.is_extant desc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'extant.asc' )
	{
	    push @clauses, "isnull(v.is_extant), v.is_extant asc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'firstapp' || $order eq 'firstapp.desc' )
	{
	    push @clauses, "v.first_early_age desc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'firstapp.asc' )
	{
	    push @clauses, "isnull(v.first_early_age), v.first_early_age asc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'lastapp' || $order eq 'lastapp.desc' )
	{
	    push @clauses, "v.last_late_age desc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'lastapp.asc' )
	{
	    push @clauses, "isnull(v.last_late_age), v.last_late_age asc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'agespan' || $order eq 'agespan.asc' )
	{
	    push @clauses, "isnull(v.first_early_age), (v.first_early_age - v.last_late_age) asc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'agespan.desc' )
	{
	    push @clauses, "(v.first_early_age - v.last_late_age) desc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'pubyr' || $order eq 'pubyr.asc' )
	{
	    push @clauses, "isnull(v.pubyr), v.pubyr asc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'pubyr.desc' )
	{
	    push @clauses, "v.pubyr desc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'author' || $order eq 'author.asc' )
	{
	    push @clauses, "isnull(v.author), v.author asc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'author.desc' )
	{
	    push @clauses, "v.author desc";
	    $tables_ref->{v} = 1;
	}
	
	elsif ( $order eq 'created' || $order eq 'created.desc' )
	{
	    push @clauses, "a.created desc";
	    $tables_ref->{a} = 1 unless $tables_ref->{use_a};
	}
	
	elsif ( $order eq 'created.asc' )
	{
	    push @clauses, "a.created asc";
	    $tables_ref->{a} = 1 unless $tables_ref->{use_a};
	}
	
	elsif ( $order eq 'modified' || $order eq 'modified.desc' )
	{
	    push @clauses, "a.modified desc";
	    $tables_ref->{a} = 1 unless $tables_ref->{use_a};
	}
	
	elsif ( $order eq 'modified.asc' )
	{
	    push @clauses, "a.modified asc";
	    $tables_ref->{a} = 1 unless $tables_ref->{use_a};
	}
	
	elsif ( $order eq 'type' || $order eq 'type.desc' || $order eq 'type.asc' )
	{
	    push @clauses, $order eq 'type.desc' ? "ref_type desc" : "ref_type asc";
	}
	
	elsif ( $order eq 'ref' || $order eq 'ref.asc' || $order eq 'ref.desc' )
	{
	    if ( $type eq 'assoc' )
	    {
		push @clauses, $order eq 'ref.desc' ? 'base.reference_no desc' : 'base.reference_no asc';
	    }
	    
	    else
	    {
		push @clauses, $order eq 'ref.desc' ? "a.reference_no desc" : "a.reference_no asc";
	    }
	}
	
	else
	{
	    croak "taxonomy: invalid order '$order'";
	}
    }
    
    return @clauses ? 'ORDER BY ' . join(', ', @clauses) : '';
}


sub ref_order {
    
    my ($taxonomy, $options) = @_;
    
    return '' unless $options->{order};
    
    my (@elements, @clauses);
    
    if ( ref $options->{order} eq 'ARRAY' )
    {
	@elements = @{$options->{order}};
    }
    
    else
    {
	@elements = split qr{\s*,\s*}, $options->{order};
    }
    
    foreach my $order (@elements)
    {
	$order = lc $order;
	
    	if ( $order eq 'author' or $order eq 'author.asc' )
	{
	    push @clauses, "r.author1last, r.author1init, ifnull(r.author2last, ''), ifnull(r.author2init,'')";
	}
	
	elsif ( $order eq 'author.desc' )
	{
	    push @clauses, "r.author1last desc, r.author1init desc, ifnull(r.author2last,'') desc, ifnull(r.author2init,'') desc";
	}
	
	elsif ( $order eq 'pubyr' or $order eq 'pubyr.asc' )
	{
	    push @clauses, "r.pubyr";
	}
	
	elsif ( $order eq 'pubyr.desc' )
	{
	    push @clauses, "r.pubyr desc";
	}
	
	elsif ( $order eq 'reftitle' or $order eq 'reftitle.asc' )
	{
	    push @clauses, "if(r.reftitle <> '', r.reftitle, r.pubtitle) asc";
	}
	
	elsif ( $order eq 'reftitle.desc' )
	{
	    push @clauses, "if(r.reftitle <> '', r.reftitle, r.pubtitle) desc";
	}
	
	elsif ( $order eq 'pubtitle' or $order eq 'pubtitle.asc' )
	{
	    push @clauses, "r.pubtitle, r.pubvol, r.pubno";
	}
	
	elsif ( $order eq 'pubtitle.desc' )
	{
	    push @clauses, "r.pubtitle desc, r.pubvol desc, r.pubno desc";
	}
	
	elsif ( $order eq 'pubtype' or $order eq 'pubtype.asc' )
	{
	    push @clauses, "r.publication_type";
	}
	
	elsif ( $order eq 'pubtype.desc' )
	{
	    push @clauses, "r.publication_type desc";
	}
	
	elsif ( $order eq 'language' or $order eq 'language.asc' )
	{
	    push @clauses, "r.language";
	}
	
	elsif ( $order eq 'language.desc' )
	{
	    push @clauses, "r.language desc";
	}
	
	elsif ( $order eq 'taxon_count' || $order eq 'taxon_count.desc' )
	{
	    push @clauses, "taxon_count desc";
	}
	
	elsif ( $order eq 'taxon_count.asc' )
	{
	    push @clauses, "taxon_count";
	}
	
	elsif ( $order eq 'created' || $order eq 'created.desc' )
	{
	    push @clauses, "r.created desc";
	}
	
	elsif ( $order eq 'created.asc' )
	{
	    push @clauses, "r.created asc";
	}
	
	elsif ( $order eq 'modified' || $order eq 'modified.desc' )
	{
	    push @clauses, "r.modified desc";
	}
	
	elsif ( $order eq 'modified.asc' )
	{
	    push @clauses, "r.modified asc";
	}
	
	else
	{
	    croak "taxonomy: invalid order '$order'";
	}
    }
    
    return @clauses ? 'ORDER BY ' . join(', ', @clauses) : '';
}


sub reftaxa_order {
    
    my ($taxonomy, $options) = @_;
    
    return 'ORDER BY base.reference_no, t.lft' unless $options->{order};
    
    my (@elements, @clauses);
    
    if ( ref $options->{order} eq 'ARRAY' )
    {
	@elements = @{$options->{order}};
    }
    
    else
    {
	@elements = split qr{\s*,\s*}, $options->{order};
    }
    
    foreach my $order (@elements)
    {
	$order = lc $order;
	
    	if ( $order eq 'hierarchy' or $order eq 'hierarchy.asc' )
	{
	    push @clauses, "t.lft asc";
	}
	
	elsif ( $order eq 'hierarchy.desc' )
	{
	    push @clauses, "t.lft desc";
	}
	
	elsif ( $order eq 'reference_no' or $order eq 'reference_no.asc' or $order eq 'reference_no.desc' )
	{
	    push @clauses, "base.reference_no";
	}
	
	elsif ( $order eq 'type' or $order eq 'type.asc' )
	{
	    push @clauses, "base.ref_type asc";
	}
	
	elsif ( $order eq 'type.desc' )
	{
	    push @clauses, "base.ref_type desc";
	}
	
	elsif ( $order eq 'name' or $order eq 'name.asc' )
	{
	    push @clauses, "t.name asc";
	}
	
	elsif ( $order eq 'name.desc' )
	{
	    push @clauses, "t.name desc";
	}
	
	else
	{
	    croak "taxonomy: invalid order '$order'";
	}
    }
    
    # return 'ORDER BY reference_no, lft' unless @clauses;
    return @clauses ? 'ORDER BY ' . join(', ', @clauses) : '';
}


sub opinion_order {
    
    my ($taxonomy, $options, $rel) = @_;
    
    return '' unless $options->{order};
    
    my (@elements, @clauses);
    
    if ( ref $options->{order} eq 'ARRAY' )
    {
	@elements = @{$options->{order}};
    }
    
    else
    {
	@elements = split qr{\s*,\s*}, $options->{order};
    }
    
    while (@elements)
    {
	my $order = lc shift @elements;
	
    	if ( $order eq 'author' or $order eq 'author.asc' )
	{
	    push @clauses, "o.author asc";
	}
	
	elsif ( $order eq 'author.desc' )
	{
	    push @clauses, "o.author desc";
	}
	
	elsif ( $order eq 'pubyr' or $order eq 'pubyr.desc' )
	{
	    push @clauses, "o.pubyr desc";
	}
	
	elsif ( $order eq 'pubyr.asc' )
	{
	    push @clauses, "o.pubyr asc";
	}
	
	elsif ( $order eq 'id' or $order eq 'id.asc' )
	{
	    push @clauses, "o.opinion_no asc";
	}
	
	elsif ( $order eq 'id.desc' )
	{
	    push @clauses, "o.opinion_no desc";
	}
	
	elsif ( $order eq 'status' or $order eq 'status.asc' )
	{
	    push @clauses, "o.status asc";
	}
	
	elsif ( $order eq 'status.desc' )
	{
	    push @clauses, "o.status desc";
	}
	
	elsif ( $order eq 'hierarchy' or $order eq 'hierarchy.asc' )
	{
	    push @clauses, "t.lft asc";
	    push @clauses, "if(opinion_type='C',0,1), o.pubyr desc" unless @elements;
	}
	
	elsif ( $order eq 'hierarchy.desc' )
	{
	    push @clauses, "t.lft desc";
	    push @clauses, "if(opinion_type='C',1,0), o.pubyr asc" unless @elements;
	}

	elsif ( $order eq 'optype' || $order eq 'optype.asc' )
	{
	    push @clauses, "if(opinion_type='C',0,1)";
	}

	elsif ( $order eq 'optype.desc' )
	{
	    push @clauses, "if(opinion_type='C',1,0)";
	}
	
	elsif ( $order eq 'name' || $order eq 'name.asc' )
	{
	    push @clauses, "t.name asc";
	}
	
	elsif ( $order eq 'name.desc' )
	{
	    push @clauses, "t.name desc";
	}
	
	elsif ( $order eq 'childname' || $order eq 'childname.asc' )
	{
	    push @clauses, "a.taxon_name asc";
	}
	
	elsif ( $order eq 'childname.desc' )
	{
	    push @clauses, "a.taxon_name desc";
	}
	
	elsif ( $order eq 'ref' || $order eq 'ref.asc' || $order eq 'ref.desc' )
	{
	    push @clauses, $order eq 'ref.desc' ? "o.reference_no desc" : "o.reference_no";
	}
	
	elsif ( $order eq 'basis' || $order eq 'basis.asc' )
	{
	    push @clauses, "o.ri asc";
	}
	
	elsif ( $order eq 'basis.desc' )
	{
	    push @clauses, "o.ri desc";
	}
	
	elsif ( $order eq 'created' || $order eq 'created.desc' )
	{
	    push @clauses, "oo.created desc";
	    # $tables_hash->{oo} = 1;
	}
	
	elsif ( $order eq 'created.asc' )
	{
	    push @clauses, "oo.created asc";
	    # $tables_hash->{oo} = 1;
	}
	
	elsif ( $order eq 'modified' || $order eq 'modified.desc' )
	{
	    push @clauses, "oo.modified desc";
	    # $tables_hash->{oo} = 1;
	}
	
	elsif ( $order eq 'modified.asc' )
	{
	    push @clauses, "oo.modified asc";
	    # $tables_hash->{oo} = 1;
	}
	
	else
	{
	    croak "taxonomy: invalid order '$order'";
	}
    }
    
    # order by t.lft, if(base.opinion_type='C',0,1), o.pubyr desc
    return @clauses ? 'ORDER BY ' . join(', ', @clauses) : '';
}


sub simple_limit {

    my ($taxonomy, $options) = @_;
    
    if ( $options->{offset} && $options->{offset} ne '' )
    {
	my $offset = $options->{offset} + 0;
	
	croak "bad offset '$options->{offset}'" unless $options->{offset} =~ /^\d+$/;
	
	if ( $options->{limit} && $options->{limit} ne '' && lc $options->{limit} ne 'all' )
	{
	    my $limit = $options->{limit} + 0;
	    
	    croak "bad limit '$options->{limit}'" unless $options->{limit} =~ /^\d+$/;
	    
	    return $offset > 0 ? "LIMIT $offset, $limit" : "LIMIT $limit";
	}
	
	else
	{
	    return $offset > 0 ? "LIMIT $offset, 999999999" : "";
	}
    }
    
    elsif ( defined $options->{limit} && $options->{limit} ne '' && lc $options->{limit} ne 'all' )
    {
	my $limit = $options->{limit} + 0;
	
	croak "bad limit '$options->{limit}'" unless $options->{limit} =~ /^\d+$/;
	
	return "LIMIT $limit";
    }
    
    else
    {
	return "";
    }
}


sub taxon_joins {

    my ($taxonomy, $mt, $tables_hash) = @_;
    
    my $joins = '';
    
    $joins .= "\t\tLEFT JOIN $taxonomy->{INTS_TABLE} as ph on ph.ints_no = $mt.ints_no\n"
	if $tables_hash->{ph};
    $joins .= "\t\tLEFT JOIN $taxonomy->{LOWER_TABLE} as pl on pl.orig_no = $mt.orig_no\n"
	if $tables_hash->{pl};
    $joins .= "\t\tLEFT JOIN $taxonomy->{COUNTS_TABLE} as pc on pc.orig_no = $mt.synonym_no\n"
	if $tables_hash->{pc};
    $joins .= "\t\tLEFT JOIN $taxonomy->{TREE_TABLE} as pt on pt.orig_no = $mt.senpar_no\n"
	if $tables_hash->{pt};
    $joins .= "\t\tLEFT JOIN $taxonomy->{TREE_TABLE} as ipt on ipt.orig_no = $mt.immpar_no\n"
	if $tables_hash->{ipt};
    
    if ( $tables_hash->{tt} )
    {
	$joins .= "\t\tLEFT JOIN $taxonomy->{AUTH_TABLE} as tta on tta.taxon_no = a.type_taxon_no\n";
	$joins .= "\t\tLEFT JOIN $taxonomy->{TREE_TABLE} as tt on tt.orig_no = tta.orig_no\n";
    }
    
    $joins .= "\t\tLEFT JOIN $taxonomy->{TREE_TABLE} as vt on vt.orig_no = $mt.accepted_no\n"
	if $tables_hash->{vt} || $tables_hash->{e};
    $joins .= "\t\tLEFT JOIN $taxonomy->{ATTRS_TABLE} as v on v.orig_no = $mt.orig_no\n"
	if $tables_hash->{v};
    $joins .= "\t\tLEFT JOIN $INTERVAL_MAP as app on app.early_age = v.first_early_age
		and app.late_age = v.last_late_age and app.scale_no = 1\n"
	if $tables_hash->{app};
    $joins .= "\t\tLEFT JOIN $taxonomy->{NAMES_TABLE} as n on n.taxon_no = $mt.spelling_no\n"
	if $tables_hash->{n};
    $joins .= "\t\tLEFT JOIN $taxonomy->{NAMES_TABLE} as nn on nn.taxon_no = a.taxon_no\n"
	if $tables_hash->{nn};
    $joins .= "\t\tLEFT JOIN $taxonomy->{ECOTAPH_TABLE} as e on e.orig_no = vt.orig_no\n"
	if $tables_hash->{e};
    $joins .= "\t\tLEFT JOIN $taxonomy->{ETBASIS_TABLE} as etb on etb.orig_no = vt.orig_no\n"
	if $tables_hash->{etb};
    
    # $joins .= "\t\tLEFT JOIN $taxonomy->{REFS_TABLE} as r on r.reference_no = a.reference_no\n"
    # 	if $tables_hash->{r};
    
    return $joins;
}


sub ref_joins {

    my ($taxonomy, $mt, $tables_hash) = @_;
    
    my $joins = '';
    
    $joins .= "\t\tLEFT JOIN $taxonomy->{REFS_TABLE} as r on r.reference_no = a.reference_no\n"
    	if $tables_hash->{r};
    
    # We can add additional joins here if they become necessary (i.e. because
    # filters or order clauses requiring them have been added.)
    
    return $joins;
}


sub opinion_joins {
    
    my ($taxonomy, $mt, $tables_hash) = @_;
    
    my $joins = '';
    
    $joins .= "\t\tJOIN $taxonomy->{OP_TABLE} as oo on oo.opinion_no = $mt.opinion_no\n"
	if $tables_hash->{oo};
    $joins .= "\t\tLEFT JOIN $taxonomy->{TREE_TABLE} as pt on pt.orig_no = $mt.parent_no\n"
	if $tables_hash->{pt};
    $joins .= "\t\tLEFT JOIN $taxonomy->{ATTRS_TABLE} as cv on cv.orig_no = $mt.orig_no\n"
	if $tables_hash->{cv};
    $joins .= "\t\tLEFT JOIN $taxonomy->{ATTRS_TABLE} as pv on pv.orig_no = $mt.parent_no\n"
	if $tables_hash->{pv};
    
    return $joins;
}


# sub auth_join {
    
#     my ($taxonomy, $options) = @_;
    
#     if ( $options->{all_variants} )
#     {
# 	return "\t\tJOIN $taxonomy->{AUTH_TABLE} as a on a.orig_no = t.orig_no\n";
#     }
    
#     else
#     {
# 	return "\t\tJOIN $taxonomy->{AUTH_TABLE} as a on a.taxon_no = t.spelling_no\n";
#     }
# }


sub order_result_list {
    
    my ($taxonomy, $result, $base_list) = @_;
    
    my (%base_list, @base_nos, %uniq, %exclude);
    
    return unless ref $base_list eq 'ARRAY';
    
    foreach my $r ( @$result )
    {
	push @{$base_list{$r->{base_no}}}, $r;
    }
    
    @$result = ();
    
    if ( ref $base_list eq 'ARRAY' && ref $base_list->[0] )
    {
	@base_nos = grep { $uniq{$_} ? 0 : ($uniq{$_} = 1) }
	    map { $_->{taxon_no} || $_->{orig_no} } @$base_list;
    }
    
    elsif ( ref $base_list eq 'ARRAY' )
    {
	@base_nos = grep { $uniq{$_} ? 0 : ($uniq{$_} = 1) } @$base_list;
    }
    
    elsif ( ref $base_list eq 'HASH' )
    {
	return;
    }
    
    else
    {
	@base_nos = grep { $uniq{$_} ? 0 : ($uniq{$_} = 1) } split( qr{\s*,\s*}, $base_list);
    }
    
    foreach my $b ( @base_nos )
    {
	push @$result, @{$base_list{$b}} if $base_list{$b};
    }
}


# compute_ancestry ( base_nos, immediate )
# 
# Use the ancestry scratch table to compute the set of common parents of the
# specified taxa (a stringified list of identifiers).  If $immediate is true,
# then list immediate parents rather than the senior synonyms of parents.
# 
# This function is only necessary because MySQL stored procedures cannot work
# on temporary tables.  :(

sub compute_ancestry {

    my ($taxonomy, $base_string, $immediate, $options) = @_;
    
    my $dbh = $taxonomy->{dbh};
    my $AUTH_TABLE = $taxonomy->{AUTH_TABLE};
    my $TREE_TABLE = $taxonomy->{TREE_TABLE};
    my $SCRATCH_TABLE = $taxonomy->{SCRATCH_TABLE};
    
    my $result;
    
    # Create a temporary table by which we can extract information from
    # the scratch table and convey it past the table locks.
    
    $result = $dbh->do("DROP TABLE IF EXISTS ancestry_temp");
    $result = $dbh->do("CREATE TEMPORARY TABLE ancestry_temp (
				orig_no int unsigned primary key,
				is_base tinyint unsigned) Engine=MyISAM");
    
    # Generate the SQL string to fill the scratch table, and output it as a debug line if necessary.
    
    my $imm = $immediate ? '_immediate' : '';
    
    my $sql = "CALL compute_ancestry$imm('$AUTH_TABLE','$TREE_TABLE', '$base_string')";
    
    if ( ref $options->{debug_out} eq 'CODE' )
    {
	&{$options->{debug_out}}("$sql\n");
    }
    
    # Lock the tables that will be used by the stored procedure.
    
    $result = $dbh->do("LOCK TABLES $SCRATCH_TABLE write,
				    $SCRATCH_TABLE as s write,
				    $AUTH_TABLE read,
				    $TREE_TABLE read,
				    ancestry_temp write");
    
    # We need a try block to make sure that the table locks are released
    # no matter what else happens.
    
    try
    {	
	$result = $dbh->do($sql);
	
	# Now copy the information out of the scratch table to a temporary
	# table so that we can release the locks.
	
	$result = $dbh->do("INSERT INTO ancestry_temp SELECT * FROM $SCRATCH_TABLE"); 
    }
    
    catch {
        die $_ if $_;
    }
    
    finally {
	$dbh->do("UNLOCK TABLES");
    };
    
    # There is no need to return anything, since the results of this function
    # are in the rows of the 'ancestry_temp' table.  But we can stop here on
    # debugging.
    
    my $a = 1;
}


# add_ancestry ( table_name )
# 
# Take all of the orig_no values from the specified table, then add to the
# table the orig_no values for all taxa ancestral to these.
# 
# This function is only necessary because MySQL stored procedures cannot work
# on temporary tables.  :(

sub add_ancestry {

    my ($taxonomy, $table_name, $options) = @_;
    
    my $dbh = $taxonomy->{dbh};
    my $TREE_TABLE = $taxonomy->{TREE_TABLE};
    my $SCRATCH_TABLE = $taxonomy->{SCRATCH_TABLE};
    
    my $result;
    
    # Generate the SQL statement that will fill the scratch table, and output it as a debug line
    # if necessary.
    
    my $sql = "CALL compute_ancestry_2('$TREE_TABLE', '$table_name')";
    
    if ( ref $options->{debug_out} eq 'CODE' )
    {
	&{$options->{debug_out}}("$sql\n");
    }
    
    # Lock the tables that will be used by the stored procedure
    # "compute_ancestry".
    
    $result = $dbh->do("LOCK TABLES $SCRATCH_TABLE write,
				    $SCRATCH_TABLE as s write,
				    $TREE_TABLE read,
				    $table_name write");
    
    # We need a try block to make sure that the table locks are released
    # no matter what else happens.
    
    try
    {
	# Fill the scratch table with the requested ancestry list.
	
	$result = $dbh->do($sql);
	
	# Now copy the ancestral orig_no values back to the specified table.
	
	$result = $dbh->do("INSERT IGNORE INTO $table_name (orig_no)
			    SELECT orig_no FROM $SCRATCH_TABLE WHERE is_base = 0");
    }
    
    catch {
        die $_ if $_;
    }
    
    finally {
	$dbh->do("UNLOCK TABLES");
    };
    
    # There is no need to return anything, since the results of this function
    # are in the rows of the 'ancestry_temp' table.  But we can stop here on
    # debugging.
    
    my $a = 1;
}



# Define sets of fields.  Each of the following identifiers can be used to
# select fields to be returned.

our (%FIELD_LIST) = ( ID => ['t.orig_no'],
		      SIMPLE => ['t.spelling_no as taxon_no', 't.orig_no', 't.name as taxon_name',
				 't.rank as taxon_rank', 't.lft', 't.rgt', 't.status', 't.immpar_no', 
				 't.senpar_no', 't.accepted_no'],
		      AUTH_SIMPLE => ['a.taxon_no', 'a.orig_no', 'a.taxon_name', 
				      '(a.taxon_rank + 0) as taxon_rank', 't.lft', 't.spelling_no',
				      't.status', 't.immpar_no', 't.senpar_no', 't.accepted_no'],
		      DATA => ['t.spelling_no as taxon_no', 't.orig_no', 't.name as taxon_name',
			       't.rank as taxon_rank', 't.lft', 't.rgt', 't.status', 't.accepted_no',
			       't.immpar_no', 't.senpar_no', 'a.common_name', 'a.reference_no',
			       'vt.name as accepted_name', 'vt.rank as accepted_rank',
			       'v.n_occs', 'v.is_extant', 'v.is_trace', 'v.is_form'],
		      AUTH_DATA => ['a.taxon_no', 'a.orig_no', 'a.taxon_name', 't.spelling_no',
				    '(a.taxon_rank + 0) as taxon_rank',
				    't.lft', 't.rgt', 't.status', 't.accepted_no', 't.immpar_no', 't.senpar_no',
				    'a.common_name', 'a.reference_no', 'vt.name as accepted_name', 
				    'nn.spelling_reason', 'n.spelling_reason as accepted_reason',
				    'vt.rank as accepted_rank', 'v.n_occs', 'v.is_extant', 'v.is_trace', 'v.is_form'],
		      REFTAXA_DATA => ['base.reference_no', 'group_concat(distinct base.ref_type) as ref_type', 
				       'a.taxon_no', 'base.orig_no', 'a.taxon_name', 'a.taxon_rank',
				       'max(base.class_no) as class_no',
				       'max(base.unclass_no) as unclass_no',
				       'max(base.occurrence_no) as occurrence_no',
				       'max(base.collection_no) as collection_no',
				       't.lft', 't.status', 't.spelling_no', 't.accepted_no',
				       't.immpar_no', 't.senpar_no', 'vt.name as accepted_name', 
				       'nn.spelling_reason', 'n.spelling_reason as accepted_reason',
				       'vt.rank as accepted_rank',
				       'v.n_occs', 'v.is_extant', 'v.is_trace', 'v.is_form'],
		      REFTAXA_SIMPLE => ['base.reference_no', 'group_concat(distinct base.ref_type) as ref_type', 
					 'base.taxon_no', 'base.orig_no', 'a.taxon_name', 'a.taxon_rank',
					 'max(base.class_no) as class_no',
					 'max(base.unclass_no) as unclass_no',
					 'max(base.occurrence_no) as occurrence_no',
					 'max(base.collection_no) as collection_no'],
		      REF_DATA => ['r.reference_no', 'r.author1init as r_ai1', 'r.author1last as r_al1', 
				   'r.author2init as r_ai2', 'r.author2last as r_al2', 'r.otherauthors as r_oa', 
				   'r.pubyr as r_pubyr', 'r.reftitle as r_reftitle', 'r.pubtitle as r_pubtitle', 
				   'r.editors as r_editors', 'r.pubvol as r_pubvol', 'r.pubno as r_pubno', 
				   'r.firstpage as r_fp', 'r.lastpage as r_lp', 'r.publication_type as r_pubtype', 
				   'r.language as r_language', 'r.doi as r_doi', 'r.comments as r_comments'],
		      REF_COUNTS => ['count(distinct taxon_no) as n_reftaxa',
				     'count(distinct auth_no) as n_refauth',
				     'count(distinct var_no) as n_refvar',
				     'count(distinct class_no) as n_refclass',
				     'count(distinct unclass_no) as n_refunclass',
				     'count(distinct occurrence_no) as n_refoccs',
				     'count(distinct specimen_no) as n_refspecs',
				     'count(distinct collection_no) as n_refcolls'],
		      OP_DATA => ['o.opinion_no', 'base.opinion_type', 't.orig_no', 't.name as taxon_name', 
				  'o.child_spelling_no', 'o.parent_no', 'o.parent_spelling_no', 'oo.basis',
				  'o.ri', 'o.pubyr', 'o.author', 'o.status', 'o.spelling_reason', 'o.reference_no',
				  'o.suppress', 'a.taxon_name as child_name', 'cast(a.taxon_rank as integer) as taxon_rank',
				  'ap.taxon_name as parent_name', 'pt.spelling_no as parent_current_no'],
		      SEARCH => ['t.orig_no', 't.name as taxon_name', 't.rank as taxon_rank',
				 't.lft', 't.rgt', 't.senpar_no'],
		      AUTH_SEARCH => ['s.taxon_no', 's.orig_no', 'a.taxon_name', 'a.taxon_rank',
				      't.lft', 't.rgt', 't.senpar_no'],
		      NEW_SEARCH => ['s.orig_no', 's.taxon_no', 't.rank as taxon_rank',
				     "if(s.is_exact, a.taxon_name, if(s.genus <> '', " .
				     "concat(s.genus, ' ', s.taxon_name), s.taxon_name)) as taxon_name",
				     't.lft', 't.rgt', 't.senpar_no'],
		      SEQ => ['t.lft', 't.rgt'],
		      RANK => ['t.min_rank', 't.max_rank'],
		      RANGE => ['t.orig_no', 't.rank as taxon_rank', 't.lft', 't.rgt'],
		      LINK => ['t.synonym_no', 't.accepted_no', 't.immpar_no', 't.senpar_no'],
		      APP => ['v.first_early_age as firstapp_ea', 
			      'v.first_late_age as firstapp_la',
			      'v.last_early_age as lastapp_ea',
			      'v.last_late_age as lastapp_la', # ],
			      'app.early_interval', 'app.late_interval'],
		      ATTR => ['v.pubyr', 'v.attribution as taxon_attr'],
		      OP_ATTR => ['cv.pubyr as taxon_pubyr', 'cv.attribution as taxon_attr', 
				  'pv.pubyr as parent_pubyr', 'pv.attribution as parent_attr'],
		      SENPAR => ['pt.name as senpar_name', 'pt.rank as senpar_rank'],
		      IMMPAR => ['ipt.name as immpar_name', 'ipt.rank as immpar_rank'],
		      SIZE => ['v.taxon_size', 'v.extant_size', 'v.n_occs'],
		      CLASS => ['ph.kingdom_no', 'ph.kingdom', 'ph.phylum_no', 'ph.phylum', 
				'ph.class_no', 'ph.class', 'ph.order_no', 'ph.order', 
				'ph.family_no', 'ph.family'],
		      GENUS => ['pl.genus_no', 'pl.genus', 'pl.subgenus_no', 'pl.subgenus'],
		      TYPE_TAXON => [ 'tt.name as type_taxon', 'tt.orig_no as type_taxon_no' ],
		      COUNTS => ['pc.order_count as n_orders', 'pc.family_count as n_families',
				 'pc.genus_count as n_genera', 'pc.species_count as n_species'],
		      TAPHONOMY => ['e.composition', 'e.thickness', 'e.architecture',
				    'e.skeletal_reinforcement as reinforcement'],
		      TAPHBASIS => ['etb.taphonomy_basis', 'etb.taphonomy_basis_no'],
		      ECOSPACE => ['e.taxon_environment', 'e.motility', 'e.vision', 'e.life_habit', 
				   'e.diet', 'e.reproduction', 'e.ontogeny', 'e.comments as ecospace_comments'],
		      ECOBASIS => ['etb.environment_basis', 'etb.motility_basis', 'etb.vision_basis',
				   'etb.life_habit_basis', 'etb.diet_basis', 'etb.reproduction_basis',
				   'etb.ontogeny_basis', 'etb.environment_basis_no', 'etb.motility_basis_no',
				   'etb.vision_basis_no', 'etb.life_habit_basis_no', 'etb.diet_basis_no',
				   'etb.reproduction_basis_no', 'etb.ontogeny_basis_no'],
		      PRES => ['v.is_trace', 'v.is_form'],
		      CRMOD => ['a.created', 'a.modified'],
		      REF_CRMOD => ['r.created', 'r.modified'],
		      OP_CRMOD => ['oo.created', 'oo.modified'],
		      AUTHENT => ['a.authorizer_no', 'a.enterer_no', 'a.modifier_no'],
		      REF_AUTHENT => ['r.authorizer_no', 'r.enterer_no', 'r.modifier_no'],
		      OP_AUTHENT => ['oo.authorizer_no', 'oo.enterer_no', 'oo.modifier_no'],
		      family_no => ['ph.family_no'],
		      image_no => ['v.image_no'],
		    );

our (%FIELD_TABLES) = ( DATA => ['v', 'vt'],
			AUTH_DATA => ['v', 'vt', 'n', 'nn'],
			REFTAXA_DATA => ['v', 'vt', 'n', 'nn'],
			REF_DATA => ['r'],
			OP_DATA => ['o', 'oo', 'pt'],
			REF_COUNTS => ['refcounts'],
			APP => ['v','app'], 
			ATTR => ['v'],
			OP_ATTR => ['cv', 'pv'],
			SIZE => ['v'],
			CLASS => ['ph'],
			GENUS => ['pl'],
			TYPE_TAXON => ['tt'],
			SUBGENUS => ['pl'],
			COUNTS => ['pc'],
			TAPHONOMY => ['e'],
			ECOSPACE => ['e'],
			TAPHBASIS => ['etb'],
			ECOBASIS => ['etb'],
			PRES => ['v'],
			SENPAR => ['pt'],
			IMMPAR => ['ipt'],
			REF_CRMOD => ['r'],
			OP_CRMOD => ['oo'],
			REF_AUTHENT => ['r'],
			OP_AUTHENT => ['oo'],
			family_no => ['ph'],
			image_no => ['v'],
		      );

1;

