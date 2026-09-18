#!/usr/bin/env perl
#
# disentangle.pl - disentangle two or more PBDB taxa.
#
#
# created by Michael McClennen
# 2026-06-02



use strict;

use lib 'lib';
use utf8;

use CoreFunction qw(loadConfig configData connectDB);
use TableDefs qw(%TABLE);
use CoreTableDefs;
use DBQuery qw(DBHashQuery DBRowQuery);
use Carp qw(carp croak);

use PBDBEdit;
use Permissions;

use Getopt::Long qw(:config bundling no_auto_abbrev permute);
use YAML;

use Term::ReadLine;

use feature 'say';
use feature 'try';

no warnings 'experimental';

# Look for command-line options.

my ($opt_quiet, $opt_verbose, $opt_config, $opt_debug, $opt_help, $opt_authorizer);

GetOptions("quiet|q" => \$opt_quiet,
	   "verbose|v" => \$opt_verbose,
	   "config|f" => \$opt_config,
	   "authorizer|a=i" => \$opt_authorizer,
	   "help|h" => \$opt_help,
	   "debug|D" => \$opt_debug) or die;

# Connect to the database.

loadConfig($opt_config);

my $dbconf = configData('Database');

if ( $ENV{PWD} ne '/var/paleomacro/pbapi' )
{
    $dbconf->{host} = '127.0.0.1';
}

my $pbdb = connectDB($opt_config, 'pbdb');

DBQuery::DebugMode(1) if $opt_debug;

# Configure the input/output. This varies depending on whether we are running with the
# Perl debugger or not.

our ($TERM);

if ( $DB::OUT )
{
    $DB::OUT->autoflush(1);
}

else
{
    $TERM = Term::ReadLine->new('disentangle');
    $TERM->enableUTF8 if $TERM->isa('Term::ReadLine::Gnu');
}

# Validate the arguments.

die "You must specify at least two taxon names to disentangle\n" unless @ARGV > 1;

# Authenticate to the database.

our ($authorizer_no) = $opt_authorizer || $ENV{AUTHORIZER_NO};

unless ( $authorizer_no )
{
    print "What authorizer number shall be used for these updates? ";
    $authorizer_no = <STDIN>;
    $authorizer_no += 0;
}

unless ( $authorizer_no > 0 )
{
    die "Invalid authorizer_no '$authorizer_no'\n";
}

our ($session_id) = DBRowQuery($pbdb, "SELECT session_id FROM $TABLE{SESSION_DATA}
			WHERE authorizer_no = '$authorizer_no' ORDER BY record_date desc LIMIT 1");

unless ( $session_id )
{
    die "You must log in to the PBDB first.\n";
}

our $perms = Permissions->new($pbdb, $session_id) ||
    die "Could not establish permissions: $!\n";
    

# Ensure that our updates are logged to the proper datalog file.

PBDBEdit->log_filename('./datalogs/datalog-DATE.sql');


# Execute the main subroutine, which includes a command loop.

our ($DONE, @OPINIONS, @TAXA, @REFERENCES, %SPECIFIED_TAXON_NO, $NONE_NEEDED, $ORIG_OK);

&DisentangleNames(@ARGV);

exit;


sub DisentangleNames {

    my (@names) = @_;
    
    my ($sql, $result);
    
    # Fetch the specified authority records, or die if they cannot be found or if one is
    # ambiguous.
    
    foreach my $i ( 0..$#names )
    {
	my $result = FetchTaxon($names[$i]);
	
	if ( @$result == 1 )
	{
	    push @TAXA, $result->[0];
	    $SPECIFIED_TAXON_NO{$result->[0]{taxon_no}} = 1;
	}
	
	elsif ( @$result > 1 )
	{
	    my $diag = join ', ', map { $_->{taxon_no} } @$result;
	    
	    die "Taxon '$names[$i]' is ambiguous: $diag\n";
	}
	
	else
	{
	    die "Could not find taxon '$names[$i]'\n";
	}
    }
    
    # Check to see if the orig_no values all fall within the set of taxa.

    $ORIG_OK = 1;
    $NONE_NEEDED = 1;
    
    foreach my $t ( @TAXA )
    {
	unless ( $SPECIFIED_TAXON_NO{$t->{orig_no}} )
	{
	    $ORIG_OK = '';
	}
	
	unless ( $t->{taxon_no} eq $t->{orig_no} )
	{
	    $NONE_NEEDED = '';
	}
    }
    
    # Fetch all opinions where child_spelling_no is one of the taxa to be disentangled.
    
    my $opinions = FetchOpinions(\@TAXA);
    
    @OPINIONS = @$opinions;
    
    my $references = FetchReferences(\@OPINIONS);
    
    @REFERENCES = @$references;
    
    GenerateAttributions(\@OPINIONS, \@REFERENCES);
    
    SortOpinions(\@OPINIONS, \@REFERENCES);
    
    DisplayOpinions(\@OPINIONS);
    
    DisplayTaxa(\@TAXA);
    
    if ( $NONE_NEEDED )
    {
	say "No disentanglement of authority records is necessary.\n";
    }
    
    elsif ( $ORIG_OK )
    {
	say "Automatic disentanglement of authority records is possible.\n";
    }
    
    else
    {
	say "Manual disentanglement of authority records is required.\n";
    }

    while ( !$DONE )
    {
	my $prompt = "disentangle> ";
	my $input;

	if ( $DB::OUT )
	{
	    $input = <STDIN>;
	}

	else
	{
	    print "$prompt";
	    $input = $TERM->readline($prompt);
	}
	
	try {
	    HandleCommand($input) if $input =~ /\S/;
	}
	    
	catch ($e) {
	    PrintMessage($e);
	};
	
	$DONE = 1 unless defined $input;
    }

    if ( ChangesToSave() )
    {
	my $input = $TERM->readline("Save changes? ");
	
	if ( $input =~ /^y/i )
	{
	    SaveChanges();
	}
    }
}


sub FetchTaxon {

    my ($name_or_no) = @_;
    
    my $quoted = $pbdb->quote($name_or_no);
    
    if ( $name_or_no =~ /^\d+$/ )
    {
	return DBHashQuery($pbdb, <<~END_SQL);
	    SELECT * FROM $TABLE{AUTHORITY_DATA}
	    WHERE taxon_no = $quoted
	    END_SQL
    }
    
    else
    {
	return DBHashQuery($pbdb, <<~END_SQL);
	    SELECT * FROM $TABLE{AUTHORITY_DATA}
	    WHERE taxon_name = $quoted
	    END_SQL
    }
}


sub FetchOpinions {
    
    my ($taxon_list) = @_;
    
    return [ ] unless $taxon_list && @$taxon_list;
    
    if ( ref $taxon_list->[0] && $taxon_list->[0]{taxon_no} )
    {
	$taxon_list = join ', ', map { $pbdb->quote($_->{taxon_no}) } @$taxon_list;
    }
    
    else
    {
	$taxon_list = join ', ', map { $pbdb->quote($_) } @$taxon_list;
    }
    
    return DBHashQuery($pbdb, <<~END_SQL);
	SELECT o.*, ach.taxon_name as child_name,
		asp.taxon_name as spelling_name, apa.taxon_name as parent_name
	FROM $TABLE{OPINION_DATA} as o
	    left join $TABLE{AUTHORITY_DATA} as ach on ach.taxon_no = o.child_no
	    left join $TABLE{AUTHORITY_DATA} as asp on asp.taxon_no = o.child_spelling_no
	    left join $TABLE{AUTHORITY_DATA} as apa on apa.taxon_no = o.parent_spelling_no
	WHERE child_no in ($taxon_list)
	END_SQL
}


sub FetchReferences {
    
    my ($opinion_list) = @_;
    
    my $ref_list = join(', ', map { $pbdb->quote($_->{reference_no}) } @$opinion_list);

    return DBHashQuery($pbdb, <<~END_SQL);
	SELECT * FROM $TABLE{REFERENCE_DATA} WHERE reference_no in ($ref_list)
	END_SQL
}


sub GenerateAttributions {

    my ($opinions, $references) = @_;
    
    my %ref_attr;
    
    foreach my $r ( @$references )
    {
	$r->{attribution} ||= GenerateAttribution($r);

	$ref_attr{$r->{reference_no}} = $r->{attribution};
    }
    
    foreach my $o ( @$opinions )
    {
	$o->{attribution} ||= GenerateAttribution($o) || $ref_attr{$o->{reference_no}};
    }
}


sub GenerateAttribution {

    my ($record) = @_;

    if ( $record->{author1last} )
    {
	my $attr_string = $record->{author1last};
	
	if ( $record->{otherauthors} || $record->{author2last} =~ /et al/ )
	{
	    $attr_string .= " et al.";
	}
	
	elsif ( $record->{author2last} )
	{
	    $attr_string .= " and $record->{author2last}";
	}
	
	$attr_string .= $record->{pubyr} ? " $record->{pubyr}" : " ???";
	
	return $attr_string;
    }
    
    else
    {
	return '';
    }
}


sub GenerateRefAttribution {
    
    my ($references) = @_;
    
    return ( ) unless $references && @$references;
    
    my %result;
    
    foreach my $r ( @$references )
    {
	my $attr_string = $r->{author1last};
	
	if ( $r->{otherauthors} || $r->{author2last} =~ /et al/ )
	{
	    $attr_string .= " et al.";
	}
	
	elsif ( $r->{author2last} )
	{
	    $attr_string .= " and $r->{author2last}";
	}
	
	$attr_string .= " $r->{pubyr}";
	
	$result{$r->{reference_no}} = $attr_string;
    }

    return %result;
}


sub GenerateOpAttribution {

    my ($opinions) = @_;
    
    return ( ) unless $opinions && @$opinions;
    
    my %result;
    
    foreach my $o ( @$opinions )
    {
	if ( $o->{author1last} )
	{
	    my $attr_string = $o->{author1last};
	    
	    if ( $o->{otherauthors} || $o->{author2last} =~ /et al/ )
	    {
		$attr_string .= " et al.";
	    }
	    
	    elsif ( $o->{author2last} )
	    {
		$attr_string .= " and $o->{author2last}";
	    }
	    
	    $attr_string .= " $o->{pubyr}";
	    
	    $result{$o->{opinion_no}} = $attr_string;
	}
    }
    
    return %result;
}


sub SortOpinions {

    my ($opinions, $references) = @_;
    
    # Run through the references and extract publication years.
    
    my (%ref_pubyr) = map { $_->{reference_no}, $_->{pubyr} } @$references;
    
    # Sort the opinions by publication year and attribution.
    
    my sub sorter {
	
	my $ap = $a->{pubyr} || $ref_pubyr{$a->{reference_no}};
	my $bp = $b->{pubyr} || $ref_pubyr{$b->{reference_no}};
	
	return ( $ap cmp $bp || $a->{attribution} cmp $b->{attribution} );
    }
    
    @$opinions = sort sorter @$opinions;
    
    foreach my $i ( 0..$opinions->$#* )
    {
	my $o = $opinions->[$i];
	$o->{index} = $i + 1;
    }
}


sub DisplayOpinions {

    my ($opinions) = @_;
    
    unless ( $opinions && @$opinions )
    {
	say "No opinions to display";
	return;
    }
    
    # Run through the opinions and display them. Mark those that cause the entanglement.
    
    my $total_opinions = scalar(@$opinions);
    
    say "\nFound $total_opinions opinions:\n";
    
    my $entangling_opinions = 0;
    
    foreach my $o ( @$opinions )
    {
	my $n = $o->{index};
	my $bullet = '   ';

	my $child_no = $o->{update_child_no} || $o->{child_no};
	my $child_spelling_no = $o->{child_spelling_no};
	my $spelling_reason = $o->{update_spelling_reason} || $o->{spelling_reason} || '';
	
	if ( $o->{delete_me} )
	{
	    $bullet = 'DEL';
	}
	
	elsif ( $SPECIFIED_TAXON_NO{$child_no} && $SPECIFIED_TAXON_NO{$child_spelling_no} &&
	     $child_no ne $child_spelling_no )
	{
	    $bullet = '***';
	}
	
	elsif ( $o->{update_child_no} || $o->{update_spelling_reason} )
	{
	    $bullet = 'UPD';
	}
	
	my $status = $o->{status};
	
	my $child = "$o->{child_name} ($o->{child_no})";
	my $spelling = "$o->{spelling_name} ($o->{child_spelling_no})";
	my $parent = "$o->{parent_name} ($o->{parent_spelling_no})";
	
	my $ch = $spelling;
	$ch .= " >> $child" if $child_spelling_no ne $child_no;
	
	say " $n. $bullet #$o->{opinion_no} $ch $status $parent";
	say "               $spelling_reason :: $o->{attribution}";
    }
    
    print "\n";
}


sub DisplayTaxa {

    my ($taxa) = @_;
    
    unless ( $taxa && @$taxa )
    {
	say "No taxa to display.";
	return;
    }
    
    my $letter = 'a';
    
    foreach my $i ( 0..$taxa->$#* )
    {
	my $t = $taxa->[$i];
	my $n = $letter++;
	
	my $name = "$t->{taxon_name} ($t->{taxon_no})";
	my $orig = "$t->{orig_name} ($t->{orig_no})";

	my $tn = $name;
	$tn .= " >> $orig" if $t->{orig_no} ne $t->{taxon_no};
	
	say " $n. $tn";
    }
    
    print "\n";
}


sub HandleCommand {

    my ($input) = @_;
    
    if ( $input =~ qr{ ^ U \s* (.*) }xs )
    {
	DoDisentangle($1);
    }

    elsif ( $input =~ qr{ ^ D \s* (.*) }xs )
    {
	DoDelete($1);
    }
    
    elsif ( $input =~ qr{ ^ s $ }xs )
    {
	if ( ChangesToSave() )
	{
	    SaveChanges();
	    $DONE = 1;
	}
    }
    
    elsif ( $input =~ qr{ ^ l }xs )
    {
	DisplayOpinions(\@OPINIONS, \@TAXA, \@REFERENCES);
	
	DisplayTaxa(\@TAXA);
	
	if ( $NONE_NEEDED )
	{
	    say "No disentanglement of authority records is necessary.\n";
	}
	
	elsif ( $ORIG_OK )
	{
	    say "Automatic disentanglement of authority records is possible.\n";
	}
	
	else
	{
	    say "Manual disentanglement of authority records is required.\n";
	}
    }
    
    elsif ( $input =~ qr{ ^ q }xs )
    {
	$DONE = 1;
    }
    
    else
    {
	say "Unknown command: $input";
    }
}


sub PrintMessage ($) {
    
    my ($msg) = @_;
    
    say "\n$msg\n";
}


sub DoDisentangle {

    my ($arg) = @_;

    if ( $arg =~ qr{ ^ (\d+) $ }xs )
    {
	DisentangleOpinion($1);
    }
    
    elsif ( $arg =~ qr{ ^ (\d+) \s* ([a-z]) $ }xs )
    {
	DisentangleOpinion($1, $2);
    }
    
    elsif ( $arg )
    {
	$arg;
	say "Invalid argument: $arg";
    }
    
    else
    {
	say "The disentangle command requires an argument";
    }
}


sub DisentangleOpinion {

    my ($opinion_index, $taxon_index) = @_;
    
    my $o = $OPINIONS[$opinion_index - 1];
    
    my $child_no = $o->{update_child_no} || $o->{child_no};
    
    unless ( $SPECIFIED_TAXON_NO{$child_no} && $SPECIFIED_TAXON_NO{$o->{child_spelling_no}} &&
	     $child_no ne $o->{child_spelling_no} )
    {
	say "That opinion does not need to be untangled\n";
	return;
    }
    
    if ( $taxon_index )
    {
	say "Not implemented yet\n";
    }
    
    elsif ( $opinion_index && $o )
    {
	$o->{update_child_no} = $o->{child_spelling_no};
	$o->{update_child_name} = $o->{spelling_name};
	$o->{update_spelling_reason} = 'original spelling'
	    if $o->{spelling_reason} eq 'recombination';
	
	DisplayOpinions([$o]);
    }
    
    else
    {
	say "There is no opinion $opinion_index";
    }
}


sub DoDelete {

    my ($arg) = @_;
    
    if ( $arg =~ qr{ ^ (\d+) $ }xs )
    {
	DeleteOpinion($1);
    }
    
    elsif ( $arg )
    {
	say "Invalid argument: $arg";
    }
    
    else
    {
	say "The delete command requires an argument";
    }
}


sub DeleteOpinion {

    my ($opinion_index) = @_;

    my $o = $OPINIONS[$opinion_index - 1];

    if ( $opinion_index && $o )
    {
	$o->{delete_me} = 1;
	
	DisplayOpinions([$o]);
    }
    
    else
    {
	say "There is no opinion $opinion_index";
    }
}


sub ChangesToSave {

    foreach my $o ( @OPINIONS )
    {
	if ( $o->{update_child_no} || $o->{update_spelling_reason} || $o->{delete_me} )
	{
	    return 1;
	}
    }

    return '';
}


sub SaveChanges {
    
    my $edt = PBDBEdit->new($pbdb, { permission => $perms, 
				     table => 'OPINION_DATA',
				     allows => ['FIXUP_MODE'] } );
    
    foreach my $o ( @OPINIONS )
    {
	if ( $o->{delete_me} )
	{
	    my $record = { opinion_no => $o->{opinion_no} };
	    
	    $edt->delete_record($record);
	}
	
	elsif ( $o->{update_child_no} || $o->{update_spelling_reason} )
	{
	    my $record = { opinion_no => $o->{opinion_no} };
	    
	    $record->{child_no} = $o->{update_child_no}
		if $o->{update_child_no};
	    $record->{spelling_reason} = $o->{update_spelling_reason}
		if $o->{update_spelling_reason};
	    
	    $edt->update_record($record);
	}
    }
    
    if ( ! $edt->actions )
    {
	say "Nothing to update";
    }
    
    elsif ( $edt->commit )
    {
	my $update_count = scalar($edt->actions);
	say "\n  Updated/deleted $update_count opinions\n";
    }
    
    else
    {
	say "Update failed!";
    }
    
    if ( my @warnings = $edt->nonfatals )
    {
	say $_ foreach ("Warnings:", @warnings);
    }
    
    if ( my @errors = $edt->fatals )
    {
	say $_ foreach ("Errors:", @errors);
    }
}
