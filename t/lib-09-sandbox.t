#
# PBDB Data Service
# -----------------
#
# lib-09-sandbox.t : Test generate_sandbox in PB2/CommonEntry.pm, which generates the HTML
#                    page for a data entry sandbox. The data service and the request are
#                    stand-ins, so that the rules, sets, and operations of the page are
#                    known, and no server or database is needed.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;

use lib 't', '../lib', 'lib';
use Test::More tests => 6;

use PB2::CommonEntry;


# A stand-in for the data service, which knows some operations, rulesets, and sets.

package SandboxDS;

sub new { my ($class, %args) = @_; return bless { %args }, $class }

sub node_defined { exists $_[0]{nodes}{$_[1]} }

sub has_ruleset { exists $_[0]{rulesets}{$_[1]} }

sub list_rules { @{ $_[0]{rulesets}{$_[1]} || [ ] } }

sub list_set_values { @{ $_[0]{sets}{$_[1]} || [ ] } }


# A stand-in for the request, which keeps the result of the operation.

package SandboxRequest;

sub new { my ($class, $ds) = @_; return bless { ds => $ds }, $class }

sub ds { $_[0]{ds} }

sub data_result { $_[0]{output} = $_[1] }


package main;


# The rules of the test ruleset, in the form returned by HTTP::Validate's list_rules.

my $DOC_ID = "The identifier of the record. Use C<abc:12> or B<12>.";
my $DOC_NAME = "The name of the record.";
my $DOC_NOTES = "Notes about the record.";

my @RULES = ( { optional => 'record_id', alias => 'id', doc_ref => \$DOC_ID },
	      { param => 'name', alias => [ 'nam', 'title' ], doc_ref => \$DOC_NAME },
	      { optional => 'notes', note => 'textarea', doc_ref => \$DOC_NOTES },
	      { optional => 'authors', note => 'textarea,json' },
	      { required => 'kind' } );

my $DS = SandboxDS->new(nodes => { 'things/addupdate' => 1 },
			rulesets => { 'things:body' => \@RULES },
			sets => { 'things:allowances' => [ 'PROCEED', 'CREATE' ] });


# Generate a sandbox page with the specified configuration, and return it.

sub sandbox {
    
    my ($config) = @_;
    
    my $request = SandboxRequest->new($DS);
    
    PB2::CommonEntry::generate_sandbox($request, $config);
    
    return $request->{output};
}


# The configuration must be a hashref naming a known operation and a known ruleset.

subtest 'configuration errors' => sub {
    
    eval { sandbox('things/addupdate') };
    like( $@, qr{must provide a hashref}, "a configuration that is not a hashref" );
    
    eval { sandbox({ ruleset => 'things:body' }) };
    like( $@, qr{must specify an operation}, "no operation" );
    
    eval { sandbox({ operation => 'things/addupdate' }) };
    like( $@, qr{must specify a ruleset}, "no ruleset" );
    
    eval { sandbox({ operation => 'things/nosuch', ruleset => 'things:body' }) };
    like( $@, qr{Unknown operation 'things/nosuch'}, "an unknown operation" );
    
    eval { sandbox({ operation => 'things/addupdate', ruleset => 'nosuch' }) };
    like( $@, qr{Unknown ruleset 'nosuch'}, "an unknown ruleset" );
};


# The page names the operation, links to its documentation, and gives the call parameters.
# The allowances are listed if a set of them is given.

subtest 'page' => sub {
    
    my $page = sandbox({ operation => 'things/addupdate', ruleset => 'things:body' });
    
    ok( $page, "a page was generated" ) || return;
    
    like( $page, qr{<title>Sandbox for things/addupdate</title>}, "it has the title" );
    like( $page, qr{<h1>Sandbox for 'things/addupdate'</h1>}, "and the heading" );
    like( $page, qr{href="/data1.2/things/addupdate_doc.html"}, "it links to the documentation" );
    like( $page, qr{id="ds_params" size="50" value="rowcount"}, "the call parameters are 'rowcount'" );
    unlike( $page, qr{allowances}, "no allowances are listed without a set" );
    like( $page, qr{sandbox_operation = 'things/addupdate';}, "the script names the operation" );
    like( $page, qr{sandbox_sections = '1';}, "and one section" );
    like( $page, qr{</body></html>\n$}, "the page is complete" );
    
    $page = sandbox({ operation => 'things/addupdate', ruleset => 'things:body',
		      allowances => 'things:allowances', extra_params => 'vocab=pbdb&show=all' });
    
    like( $page, qr{value="rowcount&vocab=pbdb&show=all"}, "the extra parameters are added" );
    like( $page, qr{sandbox_extra = 'rowcount&vocab=pbdb&show=all';}, "in the script as well" );
    like( $page, qr{allowances, those available are: PROCEED, CREATE[.]}, "the allowances are listed" );
};


# Each rule becomes a field, labelled with its name and aliases and documented. A rule noted
# as 'textarea' gets a text area, and one noted as 'json' is listed as accepting JSON.

subtest 'fields' => sub {
    
    my $page = sandbox({ operation => 'things/addupdate', ruleset => 'things:body' });
    
    ok( $page, "a page was generated" ) || return;
    
    my @inputs = $page =~ /<input type="text" name="f0_(\w+)"/g;
    my @areas = $page =~ /<textarea class="sbtext" name="f0_(\w+)"/g;
    
    is( "@inputs", "record_id name kind", "the fields with text inputs" );
    is( "@areas", "notes authors", "the fields with text areas" );
    
    like( $page, qr{<td>record_id / id<br>}, "a label includes the alias" );
    like( $page, qr{<td>name / nam / title<br>}, "and a list of aliases" );
    
    like( $page, qr{Use <span class="dbfield">abc:12</span> or <b>12</b>[.]},
	  "C<> and B<> are converted in the documentation" );
    like( $page, qr{<td class="sbdoc">The name of the record.</td>}, "each field is documented" );
    
    like( $page, qr{sandbox_fields = \['record_id','name','notes','authors','kind'\];},
	  "the script lists every field" );
    like( $page, qr{sandbox_json = \{ "authors": 1 \};}, "and the fields that accept JSON" );
    
    unlike( $page, qr{[?][?][?]}, "no field is unnamed" );
};


# With a multiplicity, the fields are repeated in that many sections, each with its own
# field name prefix.

subtest 'sections' => sub {
    
    my $page = sandbox({ operation => 'things/addupdate', ruleset => 'things:body',
			 multiplicity => 3 });
    
    ok( $page, "a page was generated" ) || return;
    
    foreach my $i ( 0 .. 2 )
    {
	my $count = () = $page =~ /name="f${i}_/g;
	is( $count, 5, "section $i has every field" );
    }
    
    unlike( $page, qr{name="f3_}, "there is no fourth section" );
    unlike( $page, qr{name="f_}, "and no field without a section number" );
    
    like( $page, qr{sandbox_sections = '3';}, "the script gives the number of sections" );
};


# A ruleset with no rules gives a page with no fields.

subtest 'empty ruleset' => sub {
    
    local $DS->{rulesets}{'things:empty'} = [ ];
    
    my $page = sandbox({ operation => 'things/addupdate', ruleset => 'things:empty' });
    
    ok( $page, "a page was generated" ) || return;
    
    unlike( $page, qr{name="f0_}, "it has no fields" );
    like( $page, qr{sandbox_fields = \[''\];}, "and the script lists none" );
};


# The operation 'unknown' gives a page saying that no sandbox is available. No ruleset is
# needed, and the operation need not be defined.

subtest 'no sandbox' => sub {
    
    my $page = sandbox({ operation => 'unknown' });
    
    ok( $page, "a page was generated" ) || return;
    
    like( $page, qr{<title>No sandbox is available</title>}, "it has the title" );
    like( $page, qr{<h1>No sandbox is available</h1>}, "and the heading" );
    unlike( $page, qr{<form}, "it has no form" );
    like( $page, qr{</body></html>\n$}, "the page is complete" );
};
