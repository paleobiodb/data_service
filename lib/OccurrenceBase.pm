# The Paleobiology Database
# 
#   OccurrenceBase.pm
#
# The routines in this module are intended to be used both by the API and by Classic to
# handle occurrences.

package OccurrenceBase;

use strict;

use base 'Exporter';

our (@EXPORT_OK) = qw(initializeModifiers parseIdentifiedName constructIdentifiedName
		      matchIdentifiedName splitTaxonName computeTaxonMatch);

use TableDefs qw(%TABLE);
use CoreTableDefs;
use List::Util qw(any);

use namespace::clean;

our (%OCC_RESO_RE);


# initializeModifiers ( dbh )
#
# This method must be called prier to any call to 'parseIdentifiedName'. It queries the
# database for the column definitions of the '%_reso' columns from the occurrences
# table, and generates regular expressions for recognizing the corresponding modifiers.

sub initializeModifiers {

    my ($dbh) = @_;
    
    my %modifiers;
    
    # For each of the four name components, generate a regular expression to recognize
    # its modifiers.
    
    foreach my $field ( qw(genus subgenus species subspecies) )
    {
	# Fetch the column definition from the database.
	
	my ($name, $definition) = $dbh->selectrow_array("
	SHOW COLUMNS FROM $TABLE{OCCURRENCE_DATA} like '${field}_reso'");
	
	# Extract the list of allowed modifiers. Skip the empty string, and also '"',
	# and 'informal'.
	
	my @modifier_list = grep { $_ && $_ ne 'informal' && $_ ne '"' } $definition =~ /'(.*?)'/g;
	
	$modifiers{$_} = 1 foreach @modifier_list;
	
	my $regex_source = join('|', @modifier_list);
	$regex_source =~ s/([.?])/[$1]/g;
	
	# The regex for subgenus must match the '(' after the modifier.
	
	if ( $field eq 'subgenus' )
	{
	    $OCC_RESO_RE{$field} = qr{^\s*($regex_source)\s*([(].*)};
	}
	
	# The regex for 'subspecies' should match an optional period after 'var',
	# 'forma', 'morph', and 'mut'.
	
	elsif ( $field eq 'subspecies' )
	{
	    $regex_source =~ s/var/va?r?[.]?/;
	    $regex_source =~ s/forma/fo?r?m?a?[.]?/;
	    $regex_source =~ s/morph/mor?p?h?[.]?/;
	    $regex_source =~ s/mut/mu?t?[.]?/;
	    $OCC_RESO_RE{$field} = qr{^\s*($regex_source)\s*(.*)};
	}
	
	# The others are formatted as follows.
	
	else
	{
	    $OCC_RESO_RE{$field} = qr{^\s*($regex_source)\s*(.*)};
	}
    }
    
    # # Now generate an expression for quickly recognizing a name with modifiers.

    # $modifiers{sensu} = 1;
    
    # my $regex_source = join('|', keys %modifiers);
    # $regex_source =~ s/([.?])/[$1]/g;
    # $regex_source =~ s/var/va?r?[.]?/;
    # $regex_source =~ s/forma/fo?r?m?a?[.]?/;
    # $regex_source =~ s/morph/mor?p?h?[.]?/;
    # $regex_source =~ s/mut/mu?t?[.]?/;
    
    # $OCC_RESO_RE{modifiers} = qr{\b(?:$regex_source)\b};
}


# parseIdentifiedName ( identified_name, options )
#
# Parse the specified name into the various name components and modifiers. Return
# the standard eight name fields, or else an object containing an error message.
#
# Accepted options:
#
# loose       If true, bad capitalization is accepted and corrected.
# debug_out   If nonempty, it must be a blessed reference that has a 'debug_line' method.

sub parseIdentifiedName {

    my ($identified_name, $options) = @_;
    
    # The following variables will be filled in by the name parsing algorithm below.
    
    my ($genus_name, $genus_reso, $subgenus_name, $subgenus_reso);
    my ($species_name, $species_reso, $subspecies_name, $subspecies_reso);
    
    $options ||= { };
    
    # Quick check for empty names.
    
    unless ( $identified_name )
    {
	return ('', '', '', '', '', '', '', '');
    }

    unless ( $identified_name =~ /[a-zA-Z]/ )
    {
	return { error => "Could not resolve genus name" };
    }
    
    # If the name is syntactically correct without modifiers, split it and return it.

    if ( my @components = quickParseIdentifiedName($identified_name) )
    {
	$options->{debug_out}->debug_line("Quick Parse\n") if $options->{debug_out};
	
	return @components;
    }
    
    $options->{debug_out}->debug_line("Slow Parse\n") if $options->{debug_out};
    
    # Otherwise, prepare to parse the identified name by trimming whitespace.
    
    $identified_name =~ s/^\s+//;
    $identified_name =~ s/\s+$//;
    
    my $name = $identified_name;
    
    # Check for the following name modifiers:
    #
    # n. gen
    # n. subgen
    # n. sp
    # n. subsp/n. ssp
    # sensu stricto
    # sensu lato
    # s. s.
    # s. l.
    # 
    # These have an unambiguous meaning wherever they occur. They may contain arbitrary
    # whitespace. Removing them may leave extra spaces in the name, which is one reason
    # why the regexes used below accept extra whitespace. For efficiency, we start with
    # a single regexp that will recognize any of them using a permissive match. We try
    # to recognize obvious misspellings too.
    
    my ($sensu, $nperiod);
    
    if ( $name =~ / n[.] \s* [sg][a-z]+[.] | s[.] \s* [sl][.] |
		    \bsens?u? \s* strict?o? | \bsens?u? \s* lato? /xs )
    {
	if ( $name =~ /(.*)n[.]\s*gen[.](.*)/ )
	{
	    $genus_reso = "n. gen.";
	    $nperiod = "n. gen.";
	    $name = "$1 $2";
	}
	
	if ( $name =~ /(.*)n[.]\s*su?b?gen[.](.*)/ )
	{
	    $subgenus_reso = "n. subgen.";
	    $nperiod = "n. subgen.";
	    $name = "$1 $2";
	}
	
	if ( $name =~ /(.*)n[.]\s*sp[.](.*)/ )
	{
	    $species_reso = "n. sp.";
	    $nperiod = "n. sp.";
	    $name = "$1 $2";
	}
	
	if ( $name =~ /(.*)n[.]\s*(?:su?bsp|ssp)[.](.*)/ )
	{
	    $subspecies_reso = "n. ssp.";
	    $nperiod = "n. ssp.";
	    $name = "$1 $2";
	}
	
	if ( $name =~ /(.*)s[.]\s*s[.](.*)/ )
	{
	    $sensu = 'sensu stricto';
	    $name = "$1 $2";
	}

	if ( $name =~ /(.*)s[.]\s*l[.](.*)/ )
	{
	    $sensu = 'sensu lato';
	    $name = "$1 $2";
	}

	if ( $name =~ /(.*)sens?u?\s*strict?o?(.*)/ )
	{
	    $sensu = 'sensu stricto';
	    $name = "$1 $2";
	}

	if ( $name =~ /(.*)sens?u?\s*lato?(.*)/ )
	{
	    $sensu = 'sensu lato';
	    $name = "$1 $2";
	}
    }
    
    # Now deconstruct the name component by component.
    
    # Start with the genus and any qualifier that may precede it.
    
    if ( $name =~ $OCC_RESO_RE{genus} )
    {
	if ( $genus_reso )
	{
	    return { error => "Conflicting genus modifiers '$genus_reso' and '$1'" };
	}
	
	$genus_reso = $1;
	$name = $2;
    }
    
    if ( $name =~ /^\s*<(.*?\S.*?)>\s*(.*)/ )
    {
	if ( $genus_reso )
	{
	    return { error => "Conflicting genus modifiers '$genus_reso' and '<>'" };
	}
	
	$genus_reso = 'informal';
	$genus_name = $1;
	$name = $2;
    }
    
    elsif ( $name =~ /^\s*("?)([A-Za-z]+)("?)\s*(.*)/ )
    {
	if ( $genus_reso && ($1 || $3) )
	{
	    return { error => "Conflicting genus modifiers '$genus_reso' and '\"\"'" };
	}
	
	elsif ( $1 || $3 )
	{
	    $genus_reso = $1;
	}
	
	$genus_name = $2;
	$name = $4;
    }
    
    else
    {
	return { error => "Invalid genus modifier" };
    }
    
    if ( $genus_name && $genus_reso ne 'informal' )
    {
	unless ( $options->{loose} || $genus_name =~ /^[A-Z][a-z]+$/ )
	{
	    return { error => "Bad capitalization on genus name" };
	}
	
	else
	{
	    $genus_name = ucfirst(lc($genus_name));
	}
    }
    
    # Continue with a possible subgenus and preceding qualifier.
    
    if ( $name =~ $OCC_RESO_RE{subgenus} )
    {
	if ( $subgenus_reso )
	{
	    return { error => "Conflicting subgenus modifiers '$subgenus_reso' and '$1'" };
	}
	
	$subgenus_reso = $1;
	$name = $2;
    }
    
    if ( $name =~ /^[(]<(.*?\S.*?)>[)]\s*(.*)/ )
    {
	if ( $subgenus_reso )
	{
	    return { error => "Conflicting subgenus modifiers '$subgenus_reso' and '<>'" };
	}
	
	$subgenus_reso = 'informal';
	$subgenus_name = $1;
	$name = $2;
    }
    
    elsif ( $name =~ /^[(]("?)([A-Za-z]+)("?)[)]\s*(.*)/ )
    {
	if ( $subgenus_reso && ($1 || $3) )
	{
	    return { error => "Conflicting subgenus modifiers '$subgenus_reso' and '\"\"'" };
	}
	
	elsif ( $1 || $3 )
	{
	    $subgenus_reso = $1;
	}
	
	$subgenus_name = $2;
	$name = $4;
    }
    
    elsif ( $name =~ /^(.*)[(]/ )
    {
	return { error => "Could not resolve subgenus" };
    }
    
    else
    {
	$subgenus_name ||= '';
    }
    
    if ( $subgenus_name && $subgenus_reso ne 'informal' )
    {
	unless ( $options->{loose} || $subgenus_name =~ /^[A-Z][a-z]+$/ )
	{
	    return { error => "Bad capitalization on subgenus" };
	}
	
	else
	{
	    $subgenus_name = ucfirst(lc($subgenus_name));
	}
    }
    
    # Continue with a species name and any qualifier that may precede it.
    
    if ( $name =~ $OCC_RESO_RE{species} )
    {
	if ( $species_reso )
	{
	    return { error => "Conflicting species modifiers '$species_reso' and '$1'" };
	}
	
	$species_reso = $1;
	$name = $2;
    }
    
    if ( ($species_reso eq 'cf.' || $species_reso eq 'aff.') &&
	 $name =~ /([A-Z])[.]\s*(.*)/ )
    {
	if ( $1 ne substr($genus_name, 0, 1) )
	{
	    return { error => "Genus initial after $species_reso did not match genus" };
	}

	$name = $2;
    }
    
    if ( $name =~ /^<(.*?\S.*?)>\s*(.*)/ )
    {
	if ( $species_reso )
	{
	    return { error => "Conflicting species modifiers '$species_reso' and '<>'" };
	}
	
	$species_reso = 'informal';
	$species_name = $1;
	$name = $2;
    }
    
    elsif ( $name =~ /^("?)([A-Za-z.]+)("?)\s*(.*)/ )
    {
	if ( $species_reso && ($1 || $3) )
	{
	    return { error => "Conflicting species modifiers '$species_reso' and '\"\"'" };
	}
	
	elsif ( $1 || $3 )
	{
	    $species_reso = $1;
	}
	
	$species_name = $2;
	$name = $4;
    }
    
    else
    {
	return { error => "Could not resolve species" };
    }
    
    if ( $species_name && $species_reso ne 'informal' )
    {
	if ( $species_name =~ / ^ ( ss?pp?[.]? | ind(?:et)?[.]? | sens?u?[.]? | .*[.] |
				    [a-z][a-z]? | var | forma | morph | mut ) $ /x )
	{
	    if ( $species_name =~ /^ind(?:et)?[.]?$/i )
	    {
		$species_name = 'indet.';
	    }
	    
	    elsif ( $species_name =~ /^ss?p[.]?/i )
	    {
		$species_name = 'sp.';
	    }

	    elsif ( $species_name =~ /^ss?pp[.]?/i )
	    {
		$species_name = 'spp.';
	    }

	    elsif ( $species_name =~ / ^ (var | forma | morph | mut) $/x )
	    {
		return { error => "Missing species name" };
	    }
	    
	    else
	    {
		return { error => "Invalid species '$species_name'" };
	    }
	}
	
	elsif ( !( $options->{loose} || $species_name =~ /^[a-z]+$/) )
	{
	    return { error => "Bad capitalization on species name" };
	}

	else
	{
	    $species_name = lc($species_name);
	}
    }
    
    # Finish with a possible subspecies name and any qualifier that may precede it.
    
    if ( $name =~ $OCC_RESO_RE{subspecies} )
    {
	if ( $subspecies_reso )
	{
	    return { error => "Conflicting subspecies modifiers '$subspecies_reso' and '$1'" };
	}
	
	$subspecies_reso = $1;
	$name = $2;
	
	# Remove any trailing period from 'var', 'forma', 'morph' and 'mut',
	# because the database stores them bare.
	
	if ( $subspecies_reso =~ qr{^(var|forma|morph|mut)[.]$}xs )
	{
	    $subspecies_reso = $1;
	}
    }
    
    if ( $name =~ /^<(.*?\S.*?)>\s*(.*)/ )
    {
	if ( $subspecies_reso )
	{
	    return { error => "Conflicting subspecies modifiers '$subspecies_reso' and '<>'" };
	}
	
	$subspecies_reso = 'informal';
	$subspecies_name = $1;
	$name = $2;
    }
    
    elsif ( $name =~ /^("?)([A-Za-z.]+)("?)\s*(.*)/ )
    {
	if ( $subspecies_reso && ($1 || $3) )
	{
	    return { error => "Conflicting subspecies modifiers '$subspecies_reso' and '\"\"'" };
	}
	
	elsif ( $1 || $3 )
	{
	    $subspecies_reso = $1;
	}
	
	$subspecies_name = $2;
	$name = $4;
    }
    
    elsif ( $name && ! $species_name )
    {
	return { error => "Could not resolve species" };
    }
    
    elsif ( $subspecies_reso )
    {
	return { error => "Could not resolve subspecies" };
    }
    
    elsif ( $name && $name !~ /^\s+$/ )
    {
	return { error => "Could not parse '$name'" };
    }
    
    if ( $subspecies_name && $subspecies_reso ne 'informal' )
    {
	if ( $subspecies_name =~ / ^ ( ss?pp?[.]? | ind(?:et)?[.]? | sens?u?[.]? | .*[.] |
				  [a-z][a-z]? | var | form?a? | morp?h? | mut ) $ /x )
	{
	    if ( $subspecies_name =~ /^ind(?:et)?[.]?$/i )
	    {
		$subspecies_name = 'indet.';
	    }
	    
	    elsif ( $subspecies_name =~ /^ss?p?[.]?$/i )
	    {
		$subspecies_name = 'ssp.';
	    }
	    
	    elsif ( $subspecies_name =~ /^ss?pp[.]?$/i )
	    {
		$subspecies_name = 'sspp.';
	    }
	    
	    else
	    {
		return { error => "invalid subspecies '$subspecies_name'" };
	    }
	}
	
	elsif ( !( $options->{loose} || $subspecies_name =~ /^[a-z]+$/ ) )
	{
	    return { error => "Bad capitalization on subspecies" };
	}

	else
	{
	    $subspecies_name = lc($subspecies_name);
	}
    }
    
    no warnings 'uninitialized';

    if ( $subspecies_name && $subspecies_reso eq 'n. ssp.' && $subspecies_name =~ /[.]/ )
    {
	return { error => "Invalid modifier 'n. ssp.'" };
    }
    
    if ( $subspecies_name && $species_reso eq 'n. sp.' )
    {
	return { error => "Invalid modifier 'n. sp.'" };
    }
    
    if ( $subspecies_name && $subgenus_reso eq 'n. subgen.' )
    {
	return { error => "Invalid modifier 'n. subgen." };
    }
    
    if ( $subspecies_name && $genus_reso eq 'n. gen.' )
    {
	return { error => "Invalid modifier 'n. gen.'" };
    }
    
    if ( $species_name && $species_reso eq 'n. sp.' && $species_name =~ /[.]/ )
    {
	return { error => "Invalid modifier 'n. sp.'" };
    }
    
    if ( $species_name && $genus_reso eq 'n. gen.' && $species_reso ne 'n. sp.' )
    {
	return { error => "You forgot to include 'n. sp.'" };
    }
    
    if ( $species_name && $subgenus_reso eq 'n. subgen.' && $species_reso ne 'n. sp.' )
    {
	return { error => "You forgot to include 'n. sp.'" };
    }
    
    if ( $subgenus_name && $genus_reso eq 'n. gen.' )
    {
	return { error => "Invalid modifier 'n. gen.'" };
    }
    
    # If the name contained one of 'sensu stricto', 's.s.', 'sensu lato', 's.l.', then
    # stuff that modifier wherever it fits. Start with the genus modifier if that is
    # empty.

    if ( $sensu )
    {
	if ( $nperiod )
	{
	    return { error => "Conflicting modifiers '$nperiod' and '$sensu'" };
	}
	
	elsif ( ! $genus_reso )
	{
	    $genus_reso = $sensu;
	}
	
	elsif ( $species_name !~ /[.]$/ && ! $species_reso )
	{
	    $species_reso = $sensu;
	}
	
	elsif ( $subgenus_name && ! $subgenus_reso )
	{
	    $subgenus_reso = $sensu;
	}

	elsif ( $subspecies_name && ! $subspecies_reso )
	{
	    $subspecies_reso = $sensu;
	}
	
	else
	{
	    my $conflict = $subspecies_reso || $subgenus_reso || $species_reso;
	    
	    return { error => "Conflicting modifiers '$conflict' and '$sensu'" };
	}
    }
    
    # If we get here, then the name has been correctly parsed.
    
    if ( $options->{debug_out} )
    {
	my $string = join(' | ', $genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
			  $species_name, $species_reso, $subspecies_name, $subspecies_reso);

	$options->{debug_out}->debug_line("$string\n");
    }
    
    return ($genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
	    $species_name, $species_reso, $subspecies_name, $subspecies_reso);
}


# quickParseIdenifiedName ( identified_name )
#
# If the name is syntactically correct, has either a species name or else 'indet.',
# 'sp.' or 'spp.', and doesn't contain any modifiers, split it and return the components.
# Otherwise, return the empty list.

sub quickParseIdentifiedName {

    my ($name) = @_;
    
    # Check the name against the overall pattern.
    
    if ( $name =~  / ^ \s* ([A-Z][a-z]+) (?: \s+ [(]([A-Z][a-z]+)[)] )?
		     \s+ ([a-z]+|indet[.]|spp?[.]) (?: \s+ ([a-z]+|ssp[.]) )? \s* $ /xs )
    {
	my $genus = $1;
	my $subgenus = $2;
	my $species = $3;
	my $subspecies = $4;

	if ( $species =~ / ^ (var|forma|morph|mut) $ /x )
	{
	    return ();
	}
	
	# If we get here, then the name has been parsed correctly.
	
	return ($genus, '', $subgenus, '', $species, '', $subspecies, '');
    }
    
    # If the name couldn't pass the basic regex, send it to the full parser.
    
    else
    {
	return ();
    }
}


# constructIdentifiedName ( genus_name, genus_reso, ... )
#
# Given the standard eight name components, put the identified name back together in a
# syntactically correct manner.

sub constructIdentifiedName {

    my ($genus_name, $genus_reso, $subgenus_name, $subgenus_reso,
	$species_name, $species_reso, $subspecies_name, $subspecies_reso) = @_;
    
    no warnings 'uninitialized';
    
    my @end_mods;
    
    my $clean_name = combine_modifier($genus_name, $genus_reso, \@end_mods) || '';
    
    if ( $subgenus_name )
    {
	$clean_name .= " (" . combine_modifier($subgenus_name, $subgenus_reso, \@end_mods) . ")";
    }
    
    if ( $species_name )
    {
	$clean_name .= " " . combine_modifier($species_name, $species_reso, \@end_mods);
    }
    
    if ( $subspecies_name )
    {
	$clean_name .= " " . combine_modifier($subspecies_name, $subspecies_reso, \@end_mods);
    }
    
    if ( @end_mods )
    {
	$clean_name .= " " . join(' ', @end_mods);
    }
    
    return $clean_name;
}


sub combine_modifier {
    
    my ($name, $modifier, $end_mod_list) = @_;
    
    return $name unless $modifier;
    
    if ( $modifier eq 'informal' )
    {
	return "<$name>";
    }
    
    elsif ( $modifier eq '"' )
    {
	return qq{"$name"};
    }
    
    elsif ( $modifier =~ /^n[.]|^sensu/ )
    {
	push @$end_mod_list, $modifier;
	return $name;
    }
    
    else
    {
	return "$modifier $name";
    }
}


# matchIdentifiedName ( dbh, genus_name, subgenus_name, species_name, subspecies_name )
#
# Return a list of taxon numbers matching the specified name components from an
# occurrence or reidentification.
#
# If an exact match is found, it alone will be returned.
# 
# Otherwise, if a subspecies was specified and one or more matches to the species are
# found, they will be returned. Otherwise, if a species was specified and one or more
# matches to the genus/subgenus are found, they will be returned. The match algorithm
# accepts a genus match for the subgenus, and vice versa, in case the subgenus was
# promoted or the genus was demoted.

sub matchIdentifiedName {

    my ($dbh, $debug_out, $genus_name, $subgenus_name, $species_name, $subspecies_name) = @_;
    
    my ($sql, $result, @matches, @best_class, %duplicate);
    
    return () unless $genus_name;
    
    # If there are no wildcards, try for an exact match. If we get one, go on to the
    # filtering step. Look for duplicate names that have the same orig_no, because we
    # only want to return one of them.
    
    my $latin_name = $genus_name;
    $latin_name .= " ($subgenus_name)" if $subgenus_name;
    $latin_name .= " $species_name" if $species_name;
    $latin_name .= " $subspecies_name" if $subspecies_name;
    
    unless ( $latin_name =~ /[%_]/ )
    {
	my $qname = $dbh->quote($latin_name);
	
	$sql = "SELECT taxon_no, taxon_name, taxon_rank, orig_no, '40' as score
		FROM $TABLE{AUTHORITY_DATA} WHERE taxon_name = $qname";
	
	$debug_out->debug_line("$sql\n") if $debug_out;
	
	$result = $dbh->selectall_arrayref($sql, { Slice => {} });

	foreach my $match ( @$result )
	{
	    $duplicate{"$match->{taxon_name}:$match->{orig_no}"}++;
	}
    }
    
    # If didn't find an exact match, look for looser matches. These may not be
    # completely specified (i.e. only a species match with the subspecies missing, or
    # only a genus match with the species missing) or they may have the occurrence genus
    # as a subgenus or vice versa. Also handle the % and _ wildcards.

    unless ( @$result )
    {
	my ($opt_re, $req_re, $genus_re, $subgenus_re);
	
	# First generate regular expressions to match the various name components. The
	# species regular expression comes in two varieties: one where the species name
	# is required, and one where it is not. The subspecies name is always optional.
	
	if ( $genus_name =~ /[%_]/ )
	{
	    $genus_re = $genus_name =~ s/%/[a-zA-Z]*/gr;
	    $genus_re =~ s/_/[a-zA-Z]/g;
	}
	
	else
	{
	    $genus_re = $genus_name;
	}
	
	if ( $subgenus_name && $subgenus_name =~ /[%_]/ )
	{
	    $subgenus_re =~ s/%/[a-zA-Z]*/gr;
	    $subgenus_re =~ s/_/[a-zA-Z]/g;
	}
	
	else
	{
	    $subgenus_re = $subgenus_name;
	}
	
	if ( $subspecies_name )
	{
	    $opt_re = "( $species_name( $subspecies_name)?)?";
	    $req_re = " $species_name( $subspecies_name)?";
	}
	
	elsif ( $species_name )
	{
	    $opt_re = "( $species_name)?";
	    $req_re = " $species_name";
	}
	
	else
	{
	    $opt_re = '';
	    $req_re = '';
	}
	
	if ( $opt_re =~ /[%_]/ )
	{
	    $opt_re =~ s/%/[a-zA-Z]*/g;
	    $opt_re =~ s/_/[a-zA-Z]/g;
	    $req_re =~ s/%/[a-zA-Z]*/g;
	    $req_re =~ s/_/[a-zA-Z]/g;
	}
	
	# In constructing the query, we need to handle four different cases:
	
	# 1. If the identified name has a subgenus and species, we need to get the union
	# of four different queries. The first for names that start with the genus and
	# either match the subgenus exactly or else match the genus and species. The
	# second for names that start with the subgenus and either match the subgenus
	# exactly as a genus or else the subgenus as genus and the species. The third
	# for names that start with a different genus but match either the subgenus
	# exactly or the subgenus and the species. The fourth for names that start with
	# a different genus and match the given genus as a subgenus.
	
	if ( $subgenus_name && $species_name )
	{
	    $sql = "SELECT taxon_no, orig_no, taxon_name FROM $TABLE{AUTHORITY_DATA}
		WHERE taxon_name like '$genus_name%' and
		      (taxon_name rlike '^$genus_re\( [(]$subgenus_re\[)])?$opt_re\$' or
		       taxon_name rlike '^$genus_re [(][A-Za-z]+[)]$req_re\$')
		UNION
		SELECT taxon_no, orig_no, taxon_name FROM $TABLE{AUTHORITY_DATA}
		WHERE taxon_name like '$subgenus_name%' and
		      (taxon_name rlike '^$subgenus_re\$' or
		       taxon_name rlike '^$subgenus_re\( [(][A-Za-z]+[)])?$req_re\$')
		UNION
		SELECT taxon_no, orig_no, taxon_name FROM $TABLE{AUTHORITY_DATA}
		WHERE subgenus_index like '$subgenus_name' and
		      taxon_name rlike '[(]$subgenus_re\[)]$opt_re\$'
		UNION
		SELECT taxon_no, orig_no, taxon_name FROM $TABLE{AUTHORITY_DATA}
		WHERE subgenus_index like '$genus_name' and
		      taxon_name rlike '[(]$genus_re\[)]$opt_re\$'";
	}

	# 2. If the identified name has a species but no subgenus, we need to get
	# the union of just two queries. The first for names that start with the
	# genus and either match it exactly or else match the genus and species. The
	# second for names that start with a different genus and either match the
	# genus as subgenus exactly or else the genus as subgenus and the species.
    
	elsif ( $species_name )
	{
	    $sql = "SELECT taxon_no, orig_no, taxon_name FROM $TABLE{AUTHORITY_DATA}
		WHERE taxon_name like '$genus_name%' and
		      (taxon_name rlike '^$genus_re$opt_re\$' or
		       taxon_name rlike '^$genus_re [(][A-Za-z]+[)]$req_re\$')
		UNION
		SELECT taxon_no, orig_no, taxon_name FROM $TABLE{AUTHORITY_DATA}
		WHERE subgenus_index like '$genus_name' and
		      taxon_name rlike '[(]$genus_re\[)]$opt_re\$'";
	}
    
	# 3. If there is no species name, the queries are simpler.
	
	elsif ( $subgenus_name )
	{
	    $sql = "SELECT taxon_no, orig_no, taxon_name FROM $TABLE{AUTHORITY_DATA}
		WHERE taxon_name like '$genus_name%' and
		      taxon_name rlike '^$genus_re\( [(]$subgenus_re\[)])?\$'
		UNION
		SELECT taxon_no, orig_no, taxon_name FROM $TABLE{AUTHORITY_DATA}
		WHERE taxon_name like '$subgenus_name'
		      and taxon_name rlike '^$subgenus_re\$'
		UNION
		SELECT taxon_no, orig_no, taxon_name FROM $TABLE{AUTHORITY_DATA}
		WHERE subgenus_index like '$subgenus_name' and
		      taxon_name rlike '[(]$subgenus_re\[)]\$'
		UNION
		SELECT taxon_no, orig_no, taxon_name FROM $TABLE{AUTHORITY_DATA}
		WHERE subgenus_index like '$genus_name' and
		      taxon_name rlike '[(]$genus_re\[)]\$'";
	}
	
	# 4. With no subgenus name, even simpler.
	
	else
	{
	    $sql = "SELECT taxon_no, orig_no, taxon_name FROM $TABLE{AUTHORITY_DATA}
		WHERE taxon_name like '$genus_name' and
		      taxon_name rlike '^$genus_re\$'
		UNION
		SELECT taxon_no, orig_no, taxon_name FROM $TABLE{AUTHORITY_DATA}
		WHERE subgenus_index like '$genus_name' and
		      taxon_name rlike '[(]$genus_re\[)]\$'";
	}
	
	# Now execute the query
    
	$debug_out->debug_line("$sql\n") if $debug_out;
    
	$result = $dbh->selectall_arrayref($sql, { Slice => { } });
    
	# If we get no matches at all, return nothing.
    
	unless ( @$result )
	{
	    return ();
	}

	# Otherwise iterate through the results to compute the match score for each one.
	# Check to see if we found a match for both "Xxx" and "Xxx (Xxx)". If so, we
	# want to discard the second one unless the subgenus was explicitly specified in
	# which case we discard the first one. Also keep track of duplicate names with
	# the same orig_no. As above, we only want to return one from each set.
	
	my (%doubles, %singles);
	
	foreach my $match ( @$result )
	{
	    unless ( $match->{score} )
	    {
		my ($match_genus, $match_subgenus, $match_species, $match_subspecies) =
		    splitTaxonName($match->{taxon_name});
		
		$match->{score} = computeTaxonMatch($genus_name, $subgenus_name,
						    $species_name, $subspecies_name,
						    $match_genus, $match_subgenus,
						    $match_species, $match_subspecies);
		
		if ( $match_genus && $match_subgenus && $match_genus eq $match_subgenus &&
		     ! $match_species )
		{
		    $doubles{$match_genus} = 1;
		    $match->{is_double} = $match_genus;
		}
		
		elsif ( $match_genus && ! $match_subgenus && ! $match_species )
		{
		    $singles{$match_genus} = 1;
		    $match->{is_single} = $match_genus;
		}
	    }
	    
	    $duplicate{"$match->{taxon_name}:$match->{orig_no}"}++;
	}
	
	# If we found any "Xxx (Xxx)" matches, see which ones we need to keep.
	
	if ( %doubles )
	{
	    foreach my $match ( @$result )
	    {
		# If a subgenus was specified, and we have a double match, discard the single.
	    
		if ( my $component = $match->{is_single} )
		{
		    if ( $component eq $subgenus_name && $doubles{$component} )
		    {
			$match->{score} = 0;
		    }
		}
	    
		# If a subgenus was not specified, and we have a single match, discard the
		# double.
	    
		elsif ( my $component = $match->{is_double} )
		{
		    if ( $component eq $genus_name && $singles{$component} &&
			 ! $subgenus_name || $subgenus_name ne $component )
		    {
			$match->{score} = 0;
		    }
		}
	    }
	}    
    }
    
    # If we found any duplicate name/rank/orig_no, select the one corresponding to the
    # spelling_no as listed in the 'taxon_trees' table and discard the rest.
    
    foreach my $k ( keys %duplicate )
    {
	if ( $duplicate{$k} > 1 )
	{
	    my ($name, $orig_no) = split(/:/, $k);
	    
	    $sql = "SELECT spelling_no FROM $TABLE{TAXON_TREES} WHERE orig_no = '$orig_no'";
	    
	    $debug_out->debug_line("$sql\n") if $debug_out;
	    
	    my ($spelling_no) = $dbh->selectrow_array($sql);

	    if ( any { $_->{taxon_no} eq $spelling_no } @$result )
	    {
		foreach my $match ( @$result )
		{
		    if ( $match->{taxon_name} eq $name &&
			 $match->{taxon_no} ne $spelling_no )
		    {
			$match->{score} = 0;
		    }
		}
	    }
	}
    }
    
    # Sort the matches in order, discarding any with a score of 0.
    
    @matches = sort { $b->{score} <=> $a->{score} } grep { $_->{score} > 0 } @$result;
    
    # Then compute the match class of the best score and then return only those
    # matches in the same class.
    
    if ( @matches )
    {
	my $best_class = int($matches[0]{score} / 10);
	
	foreach my $match ( @matches )
	{
	    push @best_class, $match->{taxon_no} if int($match->{score} / 10) >= $best_class;
	}
	
	return @best_class if @best_class;
    }
    
    # Otherwise, return nothing.
    
    return ();
}


# splitTaxonName ( taxon_name )
#
# Split the specified name into genus, subgenus, species, and subspecies components.
# Return this list of four strings. All of them will be defined, and at least the first
# will be nonempty.

sub splitTaxonName {

    my ($name) = @_;
    
    # Try for a strict match first.
    
    if ( $name && $name =~ / ^ \s* ([A-Z][a-z]+) (?: \s+ [(]([A-Z][a-z]+)[)] )?
			     (?: \s+ ([a-z.]+) )? (?: \s+ ([a-z.]+) )? \s* $ /xs )
    {
        return ($1, $2, $3, $4);
    }
    
    # If that didn't work, try for a looser match, ignoring capitalization and allowing
    # the wildcard symbols % and _. Give the result the proper capitalization.
    
    elsif ( $name && $name =~ / ^ ([a-z%_]+) (?: \s+ [(]([a-z%_]+)[)] )?
				(?: \s+ ([a-z%_.]+) )? (?: ([a-z%_.]+) )? $ /xsi )
    {
	return (ucfirst(lc($1)), ucfirst(lc($2)), lc($3), lc($4));
    }
    
    return ("", "", "", "");
}


# computeTaxonMatch ( occ_genus, occ_subgenus, occ_species, occ_subspecies,
#		      taxon_genus, taxon_subgenus, taxon_species, taxon_subspecies )
# 
# This function takes two taxonomic names -- one from the occurrences/reids table and
# one from the authorities table -- both broken down into genus, subgenus, species, and
# subspecies. It compares how closely the two match up. The higher the number, the
# better the score. The scores fall into four classes:
# 
#   40           = exact match
# < 40 but >= 30 = subspecies level match
# < 30 but >= 20 = species level match
# < 20 but >= 10 = genus/subgenus level match
# 0 = no match

sub computeTaxonMatch {
    
    my ($occ_g, $occ_sg, $occ_sp, $occ_ssp,
	$taxon_g, $taxon_sg, $taxon_sp, $taxon_ssp) = @_;

    return 0 if $occ_g eq '' || $taxon_g eq '';
    
    if ( $taxon_ssp )
    {
        if ( $occ_g eq $taxon_g && $occ_sg eq $taxon_sg && 
	     $occ_sp eq $taxon_sp && $occ_ssp eq $taxon_ssp )
	{
            return 40; # Exact match
        }
	
	elsif ( $occ_g eq $taxon_g && $occ_sp && $occ_sp eq $taxon_sp &&
	        $occ_ssp && $occ_ssp eq $taxon_ssp )
	{
            return 38; # Genus and species and subspecies match, next best
        }
	
	elsif ( $occ_g eq $taxon_sg && $occ_sp && $occ_sp eq $taxon_sp &&
		$occ_ssp && $occ_ssp eq $taxon_ssp )
	{
            return 37; # The authorities subgenus being used a genus
        }
	
	elsif ( $occ_sg eq $taxon_g && $occ_sp && $occ_sp eq $taxon_sp &&
	        $occ_ssp && $occ_ssp eq $taxon_ssp )
	{
            return 36; # The authorities genus being used as a subgenus
        }
	
	elsif ( $occ_sg && $taxon_sg && $occ_sg eq $taxon_sg && 
		$occ_sp && $occ_sp eq $taxon_sp &&
	        $occ_ssp && $occ_ssp eq $taxon_ssp )
	{
            return 35; # Genus don't match, but subgenus/species/subspecies does, pretty weak
        }
    }
    
    elsif ( $taxon_sp )
    {
        if ( $occ_g eq $taxon_g && $occ_sg eq $taxon_sg && $occ_sp eq $taxon_sp)
	{
            return 30; # Exact match to the species level
        }
	
	elsif ( $occ_g eq $taxon_g && $occ_sp && $occ_sp eq $taxon_sp )
	{
            return 28; # Genus and species match, next best thing
        }
	
	elsif ( $occ_g eq $taxon_sg && $occ_sp && $occ_sp eq $taxon_sp )
	{
            return 27; # The authorities subgenus being used a genus
        }
	
	elsif ( $occ_sg eq $taxon_g && $occ_sp && $occ_sp eq $taxon_sp )
	{
            return 26; # The authorities genus being used as a subgenus
        }
	
	elsif ( $occ_sg && $taxon_sg && $occ_sg eq $taxon_sg && 
		$occ_sp && $occ_sp eq $taxon_sp)
	{
            return 25; # Genus don't match, but subgenus/species does, pretty weak
        }
    }
    
    elsif ( $taxon_sg )
    {
        if ( $occ_g eq $taxon_g && $occ_sg eq $taxon_sg )
	{
            return 19; # Genus and subgenus match
        }
	
	elsif ( $occ_g eq $taxon_sg )
	{
            return 17; # The authorities subgenus being used a genus
        }
	
	elsif ( $occ_sg eq $taxon_g )
	{
            return 16; # The authorities genus being used as a subgenus
        }
	
	elsif ( $occ_sg eq $taxon_sg )
	{
            return 14; # Subgenera match up but genera don't, very junky
        }
    }
    
    else
    {
        if ( $occ_g eq $taxon_g )
	{
            return 18; # Genus matches at least
        }
	
	elsif ( $occ_sg eq $taxon_g )
	{
            return 15; # The authorities genus being used as a subgenus
        }
    }
    
    return 0; # no match
}

1;
