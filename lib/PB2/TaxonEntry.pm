#  
# CollectionEntry
# 
# This role provides operations for entry and editing of fossil collections.
# 
# Author: Michael McClennen

use strict;

package PB2::TaxonEntry;

use HTTP::Validate qw(:validators);

use TableDefs qw(%TABLE);
use CoreTableDefs;
use TaxonDefs qw(%TAXON_RANK %RANK_STRING);

use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);

use Moo::Role;

our (@REQUIRES_ROLE) = qw(PB2::Authentication PB2::CommonData PB2::CommonEntry PB2::TaxonData);


# initialize ( )
# 
# This routine is called by the Web::DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # Value sets for data entry operations.
    
    $ds->define_valueset('1.2:taxa:image_choice_select' =>
	{ value => 'me' },
	    "List only taxa for which at least one of the available images",
	    "has not been considered by the currently logged-in user who makes",
	    "this request.",
	{ value => 'all' },
	    "List all taxa for which at least one of the available images",
	    "has not been considered by anyone.");
    
    # Rulesets for data entry operations.
    
    $ds->define_ruleset('1.2:taxa:image_choice_selector' =>
	{ param => 'taxon_name', valid => \&PB2::TaxonData::validNameSpec, 
	  alias => 'name' },
	    "Lists image choices for the specified taxon. If more than one taxon matches",
	    "the parameter value, all are displayed.",
	{ param => 'base_name', valid => \&PB2::TaxonData::validNameSpec },
	    "Lists image choices for the specified taxon and all of its subtaxa.",
	{ param => 'taxon_id', valid => VALID_IDENTIFIER('TID'), list => ',', 
	  bad_value => '-1', alias => 'id' },
	    "Lists image choices for the specified taxon. You may",
	    "also use the alias B<C<id>> for this parameter.",
	{ param => 'base_id', valid => VALID_IDENTIFIER('TID'), list => ',', bad_value => '-1' },
	    "Lists image choices for the specified taxon and all of its subtaxa.",
	{ param => 'all_taxa', valid => FLAG_VALUE },
	    "Lists image choices for all taxa for which images are available.",
	    "You can also use the alias B<C<all_records>> for this parameter.",
	{ at_most_one => ['taxon_name', 'taxon_id', 'base_name', 'base_id', 
			  'all_taxa'] },
	{ optional => 'rank', valid => ANY_VALUE },
	    "Return only image choices for taxa with the specified rank or range of",
	    "ranks. You can use (e.g.) 'genus-order', 'genus-', '-order', '>genus', '<order'.",
	{ optional => 'max_rank', valid => ANY_VALUE },
	    "Return only image choices for taxa with at most this rank");
    
    $ds->define_ruleset('1.2:taxa:image_choices' =>
	{ require => '1.2:taxa:image_choice_selector' },
	{ optional => 'new', valid => '1.2:taxa:image_choice_select' },
	    "If this parameter is included in the request, then only taxa with at least",
	    "one image that has not previously been considered are included. Otherwise,",
	    "all taxa with image choices that match the other parameters are included.",
	    "Accepted values are:",
	{ optional => 'multiple', valid => FLAG_VALUE },
	    "If this parameter is included in the request, then only taxa with at least",
	    "two available image choices will be included. Otherwise, taxa with only one",
	    "image choice will be included as well. This parameter does not require a value.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    $ds->define_ruleset('1.2:taxa:image_choice_body' =>
	{ mandatory => 'orig_no', valid => VALID_IDENTIFIER('TXN') },
	    "The identifier of a taxon whose representative image is being selected",
	{ mandatory => 'selected', valid => VALID_IDENTIFIER('PHP') },
	    "The identifier of the selected image, which must be chosen from the",
	    "choices presented.",
	{ optional => 'considered', valid => VALID_IDENTIFIER('PHP'), list => ',' },
	    "A list of images to mark as having been considered as part of the",
	    "choice.");
    
    # Output blocks for data entry operations
    
    $ds->define_block('1.2:taxa:image_choices' =>
	{ select => ['pc.orig_no', 't.name as taxon_name', 't.trad_rank as taxon_rank',
		     'pc.image_no as selected', 'pc.modifier_no',
		     'p1.name as modifier_name', 'pc.modified'] },
	{ set => '*', code => \&process_image_choices },
	{ output => 'orig_no', com_name => 'oid' },
	    "A unique identifier for this taxon",
	{ output => 'taxon_name', com_name => 'nam' },
	    "The scientific name of this taxon",
	{ output => 'taxon_rank', com_name => 'rnk' },
	    "The taxonomic rank of this taxon",
	{ output => 'choices', com_name => 'imch' },
	    "A list of image identifiers which are available to choose",
	{ output => 'selected', com_name => 'sel' },
	    "The currently selected image identifier for this taxon",
	{ output => 'modifier_no', com_name => 'mdi' },
	    "A unique identifier for the database contributor who made the",
	    "current selection, if any",
	{ output => 'modifier_name', com_name => 'mdf' },
	    "The name of the database contributor who made the current selection, if any",
	{ output => 'modified', com_name => 'dmd' },
	    "The date/time when the current selection was made.");
}


# list_image_choices ( )
#
# This subroutine implements the operation 'taxa/image_choices'. It lists the image
# choices available for a specified taxon or range of taxa.

sub list_image_choices {

    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    
    # First make sure we are authenticated.

    my $perms = $request->require_authentication('PHYLOPIC_CHOICE');
    
    # Process the taxonomy filters.
    
    my $taxonomy = Taxonomy->new($dbh, 'taxon_trees');
    
    my @filters;
    my $tables = { 'pc', 't' };
    
    my ($taxon_name, $taxon_field, @taxon_nos, $value, @values);
    my (@include_taxa, @exclude_taxa, $all_children, @taxon_warnings, $taxa_found);
    
    if ( $value = $request->clean_param('base_name') )
    {
	$taxon_name = $value;
	$taxon_field = 'base_name';
	$all_children = 1;
    }
    
    elsif ( $value = $request->clean_param('taxon_name') )
    {
	$taxon_name = $value;
	$taxon_field = 'taxon_name';
    }
    
    elsif ( @values = $request->safe_param_list('base_id') )
    {
	@taxon_nos = @values;
	$taxon_field = 'base_id';
	$all_children = 1;
    }
    
    elsif ( @values = $request->safe_param_list('taxon_id') )
    {
	@taxon_nos = @values;
	$taxon_field = 'taxon_id';
    }
    
    # If a name was specified, we start by resolving it.  The resolution is
    # slightly different for 'match_name' than for the others.
    
    if ( $taxon_name )
    {
	my @taxa;
	my $debug_out; $debug_out = sub { $request->{ds}->debug_line($_[0]); } if $request->debug;
	
	@taxa = $taxonomy->resolve_names($taxon_name, { fields => 'RANGE,ATTR',
							all_names => 1,
							current => 1,
							debug_out => $debug_out });
	    
	push @taxon_warnings, $taxonomy->list_warnings;
	
	@include_taxa = grep { ! $_->{exclude} } @taxa;
	@exclude_taxa = grep { $_->{exclude} } @taxa;
	
	# We only care about senior synonyms.
	
	@include_taxa = $taxonomy->list_taxa('senior', \@include_taxa,
					     { fields => 'RANGE,ATTR' });
	@exclude_taxa = $taxonomy->list_taxa('senior', \@exclude_taxa,
					     { fields => 'RANGE,ATTR' })
	    if @exclude_taxa;
    }
    
    elsif ( @taxon_nos )
    {
	@include_taxa = $taxonomy->list_taxa('senior', \@taxon_nos,
					     { fields => 'RANGE,ATTR' });
    }
    
    # Then construct the necessary filters for included taxa
    
    if ( @include_taxa && $all_children )
    {
	my @include_filters;
	
	foreach my $t ( @include_taxa )
	{
	    if ( $t->{lft} && $t->{rgt} )
	    {
		push @include_filters, "t.lft between '$t->{lft}' and '$t->{rgt}'";
		$tables->{t_lft} = 1;
	    }
	    
	    elsif ( $t->{orig_no} )
	    {
		push @include_filters, "t.orig_no = '$t->{orig_no}";
	    }
	}
	
	if ( @include_filters == 1 )
	{
	    push @filters, @include_filters;
	    $taxa_found = 1;
	}
	
	elsif ( @include_filters > 1 )
	{
	    push @filters, '(' . join(' or ', @include_filters) . ')';
	    $taxa_found = 1;
	}
    }
    
    elsif ( @include_taxa )
    {
	my $taxon_list = join ',', map { $_->{orig_no} } @include_taxa;
	push @filters, "(t.accepted_no in ($taxon_list) or t.orig_no in ($taxon_list))";
	$taxa_found = 1 if @include_taxa;
    }
    
    # Now add filters for excluded taxa.  But only if there is at least one
    # included taxon as well.
    
    if ( @exclude_taxa && @include_taxa )
    {
	my @exclude_filters;
	
	foreach my $t ( @exclude_taxa )
	{
	    if ( $t->{lft} && $t->{rgt} )
	    {
		push @exclude_filters, "t.lft not between '$t->{lft}' and '$t->{rgt}'";
		$tables->{t_lft} = 1;
	    }
	    
	    elsif ( $t->{orig_no} )
	    {
		push @exclude_filters, "t.orig_no <> '$t->{orig_no}'", "t.accepted_no <> '$t->{orig_no}'";
	    }
	}
	
	push @filters, @exclude_filters;
	$tables->{tf} = 1;
    }
    
    # Add filters for rank if specified.
    
    if ( my $rank = $request->clean_param('rank') )
    {
	my $error;
	
	if ( $rank =~ /(.*?)-(.*)/ )
	{
	    my $min_rank = $1;
	    my $max_rank = $2;
	    my $min_op = '>=';
	    my $max_op = '<=';

	    if ( $min_rank )
	    {
		if ( $min_rank =~ />(.*)/ )
		{
		    $min_op = '>';
		    $min_rank = $1;
		}
		
		if ( $max_rank =~ /<(.*)/ )
		{
		    $max_op = '<';
		    $max_rank = $1;
		}
		
		if ( $TAXON_RANK{$min_rank} )
		{
		    push @filters, "t.min_rank $min_op '$TAXON_RANK{$min_rank}'";
		}
		
		elsif ( $min_rank =~ /^\d+$/ )
		{
		    push @filters, "t.min_rank $min_op '$min_rank'";
		}
		
		else
		{
		    $error = 1;
		}
	    }

	    if ( $max_rank )
	    {
		if ( $TAXON_RANK{$max_rank} )
		{
		    push @filters, "t.max_rank $max_op '$TAXON_RANK{$max_rank}'";
		}
		
		elsif ( $max_rank =~ /^\d+$/ )
		{
		    push @filters, "t.max_rank $max_op '$max_rank'";
		}
		
		else
		{
		    $error = 1;
		}
	    }
	}
	
	else
	{
	    my $op = '=';
	    
	    if ( $rank =~ /([<>])(.*)/ )
	    {
		$op = $1;
		$rank = $2;
	    }
	    
	    if ( $TAXON_RANK{$rank} )
	    {
		push @filters, "t.trad_rank $op '$TAXON_RANK{$rank}'";
	    }
	    
	    elsif ( $rank =~ /^\d+$/ )
	    {
		push @filters, "t.trad_rank $op '$rank'";
	    }

	    else
	    {
		$error = 1;
	    }
	}
	
	if ( $error )
	{
	    die $request->exception("400", "Invalid value '$rank' for 'rank'");
	}
    }
    
    # If any warnings occurred, pass them on.
    
    $request->add_warning(@taxon_warnings) if @taxon_warnings;
    
    # Return an empty response unless we have at least one taxon selected.
    
    unless ( $taxa_found )
    {
	return $request->list_result();
    }
    
    # Generate the WHERE clause.
    
    push @filters, '1' unless @filters;
    
    my $filter_string = join(' and ', @filters);
    
    # Now process the 'new' and 'multiple' parameters.
    
    my $new = $request->clean_param('new');
    my $having = $request->clean_param('multiple') ? 'HAVING count(*) > 1' : '';
    
    # If the 'strict' parameter was given, make sure we haven't generated any warnings.
    # If the 'extid' parameter was given, turn external identifiers on or off.
    
    $request->strict_check;
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit_string = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine which fields and tables are needed to display the requested
    # information.
    
    my $fields = $request->select_string;
    
    # Generate the main query.
    
    my $sql;
    
    unless ( $new )
    {
	$sql = "SELECT $calc $fields, group_concat(pn.image_no) as choices
		FROM $TABLE{PHYLOPIC_CHOICE} as pc
		    join $TABLE{TAXON_TREES} as t using (orig_no)
		    join $TABLE{PHYLOPIC_NAMES} as pn using (orig_no)
		    left join $TABLE{PERSON_DATA} as p1 on p1.person_no = pc.modifier_no
		WHERE $filter_string
		GROUP BY orig_no $having
		ORDER BY t.lft
		$limit_string";
    }
    
    else
    {
	my $new_clause = $new eq 'me' ? "and ps.person_no = '$perms->{enterer_no}'" : '';
	
	$sql = "SELECT $calc $fields, group_concat(pn.image_no) as choices
		FROM $TABLE{PHYLOPIC_CHOICE} as pc
		    join $TABLE{TAXON_TREES} as t using (orig_no)
		    join $TABLE{PHYLOPIC_NAMES} as pn using (orig_no)
		    join (SELECT distinct orig_no FROM $TABLE{TAXON_TREES} as t
			    join $TABLE{PHYLOPIC_NAMES} as pn using (orig_no)
			    left join $TABLE{PHYLOPIC_SEEN} as ps
				on ps.image_no = pn.image_no $new_clause
			  WHERE $filter_string and ps.image_no is null) as new using (orig_no)
		    left join $TABLE{PERSON_DATA} as p1 on p1.person_no = pc.modifier_no
		GROUP BY pc.orig_no $having
		ORDER BY t.lft
		$limit_string";
    }
    
    $request->debug_line("$sql\n\n") if $request->debug;
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    $request->list_result($result);
    
    $request->sql_count_rows;
}


# process_image_choices ( )
#
# This subroutine is called once for each record generated by &list_image_choices before
# it is returned. It generates external identifiers, and splits the 'choices' field into
# an array.

sub process_image_choices {

    my ($request, $record) = @_;

    if ( $request->has_block('extids') )
    {
	$record->{orig_no} = generate_identifier('TXN', $record->{orig_no})
	    if $record->{orig_no};
	
	$record->{selected} = generate_identifier('PHP', $record->{selected})
	    if $record->{selected};
	
	my @choices = split /,/, $record->{choices};
	
	foreach my $c ( @choices )
	{
	    $c = generate_identifier('PHP', $c) if $c;
	}
	
	$record->{choices} = \@choices;
    }
    
    $record->{taxon_rank} = $RANK_STRING{$record->{taxon_rank}} if $record->{taxon_rank};
}


# update_image_choices ( )
#
# This subroutine implements the operation 'taxa/update_image_choices'.

sub update_image_choices {

    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    
    my $perms = $request->require_authentication('PHYLOPIC_CHOICE');
    
    unless ( $perms->table_permission('PHYLOPIC_CHOICE', 'admin') )
    {
	die $request->exception("401", "You must have administrative permission on ",
				"the PHYLOPIC_CHOICE table");
    }
    
    my $allowances = { };
    
    my (@records) = $request->parse_body_records({ }, '1.2:taxa:image_choice_body');
    
    if ( $request->errors )
    {
	die $request->exception("400", "Bad data");
    }
    
    my $edt = EditTransaction->new($request, { permission => $perms, 
					       table => 'PHYLOPIC_CHOICE', 
					       allows => $allowances } );

    my @result;
    
    # Iterate through the posted records. We need multiple actions for each one. First,
    # recording the selection in PHYLOPIC_CHOICE. Second, recording all of the
    # considered images in PHYLOPIC_SEEN.

  RECORD:
    foreach my $r (@records)
    {
	# Create a record for insertion or update into PHYLOPIC_CHOICE.
	
	my $r1 = { orig_no => $r->{orig_no}, image_no => $r->{selected} };
	
	$edt->insert_update_record($r1);
	
	# Make sure that the selected image is actually an available choice. If it is,
	# add the insertion record to the result list. Otherwise, add an error condition
	# to the action initiated above and go on to the next record. We use
	# 'E_NOT_FOUND' so that the user has the option to specify the 'NOT_FOUND'
	# allowance which causes this to be a warning instead of an error.
	
	my ($check) = $dbh->selectrow_array("
		SELECT image_no FROM $TABLE{PHYLOPIC_NAMES}
		WHERE orig_no = '$r->{orig_no}' and image_no = '$r->{selected}'");
	
	if ( $check )
	{
	    push @result, $r1;
	}
	
	else
	{
	    $edt->add_condition('E_NOT_FOUND', 'custom', "Image '$r->{selected}' is not " .
				"an available choice for taxon '$r->{orig_no}'");
	    next RECORD;
	}
	
	# Record all of the 'considered' images in the table PHYLOPIC_SEEN.
	
	my $considered = ref $r->{considered} eq 'ARRAY' ? $r->{considered} : [ $r->{considered} ];
	
	foreach my $image_no ( $considered->@* )
	{
	    $edt->replace_record({ image_no => $image_no, person_no => $perms->enterer_no });
	}
    }
    
    # If no errors have been detected so far, execute the queued actions inside
    # a database transaction. If any errors occur during that process, the
    # transaction will be automatically rolled back unless the NOT_FOUND or
    # PROCEED allowance was given. Otherwise, it will be automatically
    # committed.
    
    $edt->commit;
    
    # Now handle any errors or warnings that may have been generated.
    
    $request->collect_edt_warnings($edt);
    $request->collect_edt_errors($edt);
    
    if ( $edt->fatals )
    {
    	die $request->exception("400", "Bad request");
    }
    
    else
    {
	return $request->list_result(@result);
    }
}

1;
