#  
# SpecimenEntry
# 
# A role that provides for data entry and editing for specimens.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::SpecimenEntry;

use HTTP::Validate qw(:validators);

use TableDefs qw($OCC_MATRIX $SPEC_MATRIX $COLL_MATRIX $SPEC_ELEMENTS);
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);

use Taxonomy;
use TaxonDefs qw(%RANK_STRING);

use Carp qw(carp croak);
use Try::Tiny;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::CommonData PB2::ReferenceData PB2::OccurrenceData PB2::TaxonData PB2::CollectionData PB2::IntervalData);

# initialize ( )
# 
# This routine is called by the Web::DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # Start with value sets for specifying data entry options
    
    $ds->define_set('1.2:specs:conditions' =>
	{ value => 'UNKNOWN_TAXA' },
	    "Proceed with adding the specimen even if the taxon name is not known",
	    "to the database.");
    
    $ds->define_set('1.2:specs:entry_return' =>
	{ value => 'updated' },
	    "Return the new or updated specimen records. This is the default",
	{ value => 'none' },
	    "Return nothing except the status code and any warnings",
	    "or cautions that were generated.");
    
    $ds->define_set('1.2:specs:types' =>
	{ value => 'holo' },
	    "The specimen is a holotype.",
	{ value => 'para' },
	    "The specimen is paratype.",
	{ value => 'mult' },
	    "The specimen consists of more than one paratype.");
    
    # Rulesets for entering and updating data.
    
    $ds->define_ruleset('1.2:specs:basic_entry' =>
	{ optional => 'record_id', value => ANY_VALUE },
	    "You may provide a value for this attribute in any record",
	    "submitted for entry or update. This allows the data service",
	    "to accurately indicate which records generated errors or warnings.",
	    "You may specify any string, but if you submit multiple records in",
	    "one call each record should have a unique value.",
	{ optional => 'spec_id', valid => VALID_IDENTIFIER('SPM') },
	    "The identifier of the specimen to be updated. If empty,",
	    "a new specimen record will be created.",
	{ optional => 'coll_id', value => VALID_IDENTIFIER('COL') },
	    "The identifier of a collection record representing the site from",
	    "which the specimen was collected.",
	{ optional => 'inst_code', value => ANY_VALUE },
	    "The acronym or abbreviation of the institution holding the specimen.",
	{ optional => 'instcoll_code', value => ANY_VALUE },
	    "The acronym or abbreviation for the institutional collection of which",
	    "the specimen is a part.",
	{ optional => 'specimen_code', value => ANY_VALUE },
	    "The specimen code or identifier in its institutional collection.",
	{ optional => 'taxon_name', value => ANY_VALUE },
	    "The name of the taxon to which this specimen is identified.",
	    "You must either specify this OR B<C<taxon_id>>.",
	{ optional => 'taxon_id', value => VALID_IDENTIFIER('TXN') },
	    "The identifier of the taxon to which this specimen is identified.",
	    "You must either specify this OR B<C<taxoon_name>>.",
	{ at_most_one => [ 'taxon_name', 'taxon_id' ] });
    
    $ds->define_ruleset('1.2:specs:op_mod' =>
	">>The following parameters affect how this operation is carried out:",
	{ optional => 'allow', valid => '1.2:specs:conditions' },
	    "This parameter specifies a list of actions that will",
	    "be allowed to occur during processing of this request, and",
	    "not block it from completing. B<Important:> for many applications,",
	    "it is best to allow the request to block, get confirmation from",
	    "the user for each flagged condition, and if confirmed then repeat the request",
	    "with these specific actions allowed using this parameter. Accepted",
	    "values include:");
    
    $ds->define_ruleset('1.2:specs:ret_mod' =>
	">>The following parameters specify what should be returned from this",
	"operation:",
    	{ optional => 'SPECIAL(show)', valid => '1.2:specs:basic_map' },
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:specs:addupdate' =>
	{ allow => '1.2:specs:op_mod' },
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every specimen specified in the body.",
	{ allow => '1.2:specs:basic_entry' },
	{ allow => '1.2:specs:ret_mod' });
    
    $ds->define_ruleset('1.2:specs:update' =>
	{ allow => '1.2:specs:op_mod' },
	">>The following parameters may be given either in the URL or in",
	"the request body, or some in either place. If they are given",
	"in the URL, they apply to every specimen specified in the body.",
	{ allow => '1.2:specs:basic_entry' },
	{ allow => '1.2:specs:ret_mod' });
    
    $ds->define_ruleset('1.2:specs:delete' =>
	{ allow => '1.2:specs:op_mod' }, 
	">>The following parameter may be given either in the URL or in",
	"the request body. Either way, you may specify more than one value,",
	"as a comma-separated list.",
	{ param => 'spec_id', valid => VALID_IDENTIFIER('SPM'), list => ',' },
	    "The identifier(s) of the specimen(s) to delete.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
}


sub update_specimens {
    
    my ($request, $arg) = @_;
    
    
}


sub delete_specimens {
    
    
    
}
