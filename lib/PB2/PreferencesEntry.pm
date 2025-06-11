#  
# PreferencesEntry
# 
# This role provides operations for setting preferences and defaults.
# 
# Author: Michael McClennen

use strict;

package PB2::PreferencesEntry;

use HTTP::Validate qw(:validators);

use TableDefs qw(%TABLE);

use CoreTableDefs;
use ExternalIdent qw(generate_identifier %IDP VALID_IDENTIFIER);
use HTTP::Validate qw(ANY_VALUE);

use Carp qw(carp croak);

use Moo::Role;

our (@REQUIRES_ROLE) = qw(PB2::Authentication PB2::CommonData PB2::CommonEntry);


our (%prefs_no_value) = (collection_search => 1, genus_and_species_only => 1, 
			 taphonomy => 1, subgenera => 1, abundances => 1, plant_organs => 1);


# initialize ( )
# 
# This routine is called by the Web::DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    $ds->define_output_map('1.2:prefs:map' =>
	{ value => 'all' },
	    "Show all preference settings in the return value, rather than",
	    "just the ones being set.");
    
    $ds->define_block('1.2:prefs:basic' => 
	{ output => 'preference', com_name => 'nam' },
	    "The name of a preference or editing field",
	{ output => 'value', com_name => 'val' },
	    "The preference value or field default value.",
	{ output => 'status', com_name => 'sta' },
	    "The status of this preference: either 'set' or 'deleted'.");
    
    $ds->define_ruleset('1.2:prefs:set' => 
	{ optional => 'field', valid => ANY_VALUE },
	    "The name of the preference to set, or a field whose default value",
	    "we are setting.",
	{ optional => 'value', valid => ANY_VALUE, allow_empty => 1 },
	    "The preference value or default field value to set.",
	{ optional => 'SPECIAL(show)', valid => '1.2:prefs:map' },
	{ allow => '1.2:special_params' },
	    "^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.2:prefs:set_body' => 
	">>If the method is PUT or POST, the  body of this request must be either a single JSON",
	"object, or an array of",
	"JSON objects, or else a single record in C<application/x-www-form-urlencoded> format.",
	{ param => 'field', valid => ANY_VALUE },
	    "The name of a preference to set, or a field whose default value we are setting.",
	{ param => 'value', valid => ANY_VALUE },
	    "The preference value or default field value to set.");
}



sub set_preference {
    
    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single record only.
    
    my $perms = $request->require_authentication();
    
    my ($allowances, $main_params) = $request->parse_main_params('1.2:prefs:set',
								 ['field', 'value']);
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result.
    
    my (@records) = $request->parse_body_records($main_params, '1.2:prefs:set_body');
    
    my %new_prefs;
    
    foreach my $r (@records)
    {
	if ( $r->{field} && exists $r->{value} )
	{
	    $new_prefs{$r->{field}} = $r->{value};
	}
    }
    
    # Now fetch the current set of preferences from the session_data table.
    
    my $person_no = $perms->enterer_no;

    unless ( $person_no )
    {
	return $request->exception(400, "Guest users cannot set preferences");
    }
    
    my $qpers = $dbh->quote($person_no);
    
    my $sql = "SELECT preferences FROM $TABLE{PERSON_DATA} WHERE person_no = $qpers";
    
    $request->debug_line("$sql\n") if $request->debug;
    
    my ($preferences) = $dbh->selectrow_array($sql);
    
    my @pref_list = split / -:- /, $preferences;
    my (@new_list, @set_list, @carry_list);
    
    foreach my $pref ( @pref_list )
    {
	if ( $pref =~ /(.*?)=(.*)/ )
	{
	    # If a new value is set, change it in @new_list.
	    
	    if ( exists $new_prefs{$1} )
	    {
		if ( defined $new_prefs{$1} && $new_prefs{$1} ne '' )
		{
		    if ( $prefs_no_value{$1} ) {
			push @new_list, "$1";
			push @set_list, "$1=yes";
		    } else {
			push @new_list, "$1=$new_prefs{$1}";
			push @set_list, "$1=$new_prefs{$1}";
		    }
		}
		
		# If $new_prefs{$1} has an undefined or empty value, omit this
		# preference from @new_list.

		else
		{
		    push @set_list, "$1=";
		}
		
		delete $new_prefs{$1};
	    }
	    
	    # If no new value is set, retain the old value in @new_list.
	    
	    else
	    {
		push @new_list, $pref;
		push @carry_list, $pref;
	    }
	}
	
	else
	{
	    # If a new value is set, change it in @new_list.
	    
	    if ( exists $new_prefs{$pref} )
	    {
		if ( defined $new_prefs{$pref} && $new_prefs{$pref} ne '' )
		{
		    push @new_list, $pref;
		    push @set_list, $pref;
		}
		
		# If $new_prefs{$pref} has an undefined or empty value, omit this
		# preference from @new_list.

		else
		{
		    push @set_list, "$pref=";
		}
		
		delete $new_prefs{$1};
	    }
	    
	    # If no new value is set, retain the old value in @new_list.
	    
	    else
	    {
		push @new_list, $pref;
		push @carry_list, $pref;
	    }
	}
    }
    
    # Now add any remaining preference settings to @new_list.
    
    foreach my $k ( keys %new_prefs )
    {
	if ( defined $new_prefs{$k} && $new_prefs{$k} ne '' )
	{
	    if ( $prefs_no_value{$k} ) {
		push @new_list, "$k";
		push @set_list, "$k=yes";
	    } else {
		push @new_list, "$k=$new_prefs{$k}";
		push @set_list, "$k=$new_prefs{$k}";
	    }
	}
    }
    
    # Use @new_list construct the new preferences string.
    
    my $new_value = $dbh->quote( join(' -:- ', @new_list) );
    
    $sql = "UPDATE $TABLE{PERSON_DATA} SET preferences = $new_value
	    WHERE person_no = $qpers";
    
    $request->debug_line("$sql\n") if $request->debug;
    
    $dbh->do($sql);
    
    # Now generate the result, starting with @set_list.
    
    my (@output);
    
    foreach my $setting ( @set_list )
    {
	if ( $setting =~ /(.*?)=$/ )
	{
	    push @output, { preference => $1, value => '', status => 'deleted' };
	}
	
	elsif ( $setting =~ /(.*?)=(.+)/m )
	{
	    push @output, { preference => $1, value => $2, status => 'set' };
	}
    }
    
    # If 'show=all' was specified, add the list of preferences carried over.
    
    if ( $request->has_block('all') )
    {
	foreach my $setting ( @carry_list )
	{
	    if ( $setting =~ /(.*?)=(.*)/ )
	    {
		push @output, { preference => $1, value => $2 };
	    }

	    else
	    {
		push @output, { preference => $setting, value => 'true' };
	    }
	}
    }
    
    $request->list_result(@output);
}


sub set_sandbox {

    my ($request) = @_;

    $request->generate_sandbox({ operation => 'prefs/set',
				 ruleset => '1.2:prefs:set_body',
				 allowances => '',
				 extra_params => '' });
}

1;
