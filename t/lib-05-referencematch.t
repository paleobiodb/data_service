#
# Paleobiology Database library modules
# -------------------------------------
#
# This file contains unit tests for library modules that are used by the EditTransaction
# client modules and by the data service.
#
# lib-05-referencematch.t : Test ReferenceMatch.pm, which extracts the fields of
#                           bibliographic references from PBDB, Crossref, and BibJSON
#                           records, and scores how similar two references are.
#                           ReferenceEdit uses it to find duplicate references.
#
# These tests do not use the database.
#
# Added by Claude under the guidance of Michael McClennen.
#

use strict;
use utf8;

use lib 't', '../lib', 'lib';
use Test::More tests => 14;

use Unicode::Normalize;

use ReferenceMatch qw(ref_similarity ref_match author_similarity get_reftitle get_subtitle
		      get_pubtitle get_publisher get_volpages get_authorname get_authorlist
		      split_authorlist parse_authorname get_pubyr get_doi get_url
		      get_publication_type title_words);

binmode(Test::More->builder->$_, ':utf8') foreach qw(output failure_output todo_output);


# Return a list as a string, with undefined values shown as 'undef'.

sub list { return join(' | ', map { $_ // 'undef' } @_) }


# Return the author list of a record as a string.

sub authors { return join(' || ', map { list(@$_[0..1]) } get_authorlist(@_)) }


# A PBDB reference record, and copies of it with changes.

my %PBDB = ( reftitle => 'The dinosaurs of Montana', pubtitle => 'Journal of Paleontology',
	     author1last => 'Smith', author1init => 'A. B.', author2last => 'Jones',
	     author2init => 'C.', otherauthors => 'D. Brown, E. White', pubyr => 1999,
	     pubvol => 12, pubno => 3, firstpage => 100, lastpage => 120,
	     publisher => 'Society Press' );

sub pbdb { return { %PBDB, @_ } }


# clean_string trims a string, collapses its spaces, decodes character entities, removes
# HTML tags, and normalizes to Unicode NFC, because its results are stored and displayed.
# The similarity functions normalize to NFD themselves.

subtest 'clean_string' => sub {
    
    is( ReferenceMatch::clean_string('  bones of dinosaurs  '), 'bones of dinosaurs',
	"spaces are trimmed" );
    is( ReferenceMatch::clean_string(undef), '', "undef becomes the empty string" );
    is( list(ReferenceMatch::clean_string(' a ', ' b ')), 'a | b', "a list is cleaned" );
    
    is( ReferenceMatch::clean_string('bones  of   dinosaurs'), 'bones of dinosaurs',
	"spaces are collapsed" );
    is( ReferenceMatch::clean_string('caf&#233;'), NFC('café'), "a character entity is decoded" );
    is( ReferenceMatch::clean_string('the <i>bones</i>'), 'the bones', "HTML tags are removed" );
    is( ReferenceMatch::clean_string(NFD('café')), NFC('café'), "the result is in NFC" );
};


# The titles of a reference. A PBDB record without a reftitle has its title in pubtitle. A
# title with a second title in brackets gives both.

subtest 'titles' => sub {
    
    is( get_reftitle(pbdb()), 'The dinosaurs of Montana', "a PBDB title" );
    is( get_reftitle(pbdb(reftitle => '')), 'Journal of Paleontology',
	"a PBDB record without a reftitle" );
    is( list(get_reftitle({ title => ['Title one', 'Title two'] })), 'Title one | Title two',
	"Crossref titles" );
    is( get_reftitle({ title => 'A BibJSON title' }), 'A BibJSON title', "a BibJSON title" );
    is( get_reftitle({ }), undef, "no title" );
    is( list(get_reftitle({ title => '[Erster Titel] [Second title of the work]' })) =~ s/\]$//r,
	'Erster Titel | Second title of the work', "two titles in brackets" );
    is( get_reftitle({ title => 'A long title of a work on dinosaurs [in German]' }),
	'A long title of a work on dinosaurs [in German]', "a short note in brackets is kept" );
    
    is( list(get_reftitle({ reftitle => 'English title [Titre français]' })),
	'English title | ' . NFC('Titre français'), "a title with a translation in brackets" );
    
    is( list(get_subtitle({ subtitle => ['A subtitle'] })), 'A subtitle', "a Crossref subtitle" );
    is( get_subtitle({ subtitle => 'Another' }), 'Another', "a BibJSON subtitle" );
    ok( ! get_subtitle({ }), "no subtitle" );
    
    is( get_pubtitle(pbdb()), 'Journal of Paleontology', "a PBDB publication" );
    is( get_pubtitle(pbdb(reftitle => '')), '', "a PBDB record with only a pubtitle has no publication" );
    is( list(get_pubtitle({ 'container-title' => ['Journal A', 'J. A'] })), 'Journal A | J. A',
	"Crossref container titles" );
    is( get_pubtitle({ journal => 'Journal B' }), 'Journal B', "a BibJSON journal" );
    is( get_pubtitle({ booktitle => 'Book C' }), 'Book C', "a BibJSON booktitle" );
    is( get_pubtitle({ series => 'Series D' }), 'Series D', "a BibJSON series" );
    is( get_publisher(pbdb()), 'Society Press', "the publisher" );
};


# The other fields: volume, issue, and pages, the year, the DOI, the URL, and the type of
# publication.

subtest 'other fields' => sub {
    
    is( list(get_volpages(pbdb())), '12 | 3 | 100 | 120', "PBDB volume, issue, and pages" );
    is( list(get_volpages(pbdb(lastpage => ''))), '12 | 3 | 100 | 100', "a missing last page" );
    is( list(get_volpages(pbdb(firstpage => '100a', lastpage => '120'))), '12 | 3 | 100 | 120',
	"the first number of the first page" );
    is( list(get_volpages({ volume => 5, issue => 2, page => '33-44' })), '5 | 2 | 33 | 44',
	"Crossref volume, issue, and pages" );
    is( list(get_volpages({ volume => 5, number => 7, pages => '33' })), '5 | 7 | 33 | undef',
	"a BibJSON number and a single page" );
    is( list(get_volpages({ })), 'undef | undef | undef | undef', "nothing" );
    
    is( get_pubyr(pbdb()), 1999, "a PBDB year" );
    is( get_pubyr({ 'published-print' => { 'date-parts' => [[2001, 5, 3]] } }), 2001,
	"a Crossref print date" );
    is( get_pubyr({ 'published-online' => { 'date-parts' => [[2002]] } }), 2002, "an online date" );
    is( get_pubyr({ issued => '2003-01-01' }), 2003, "a date as a string" );
    is( get_pubyr({ year => 2004 }), 2004, "a BibJSON year" );
    
    is( list(get_doi({ doi => '10.1234/abc' })), '10.1234/abc', "a DOI" );
    is( list(get_doi({ DOI => '10.1234/ABC' })), '10.1234/ABC', "a DOI under 'DOI'" );
    is( list(get_doi({ URL => 'http://dx.doi.org/10.1234/abc' })), '10.1234/abc', "a DOI in a URL" );
    is( list(get_doi({ identifier => [ { type => 'doi', id => '10.1/x' }, { type => 'isbn', id => '9' } ] })),
	'10.1/x', "a DOI among the identifiers" );
    is( list(get_doi({ doi => '10.1/x', aliases => ['10.2/y', 'other'] })), '10.1/x | 10.2/y',
	"a DOI and its aliases" );
    is( scalar(() = get_doi({ doi => 'none' })), 0, "a value that is not a DOI" );
    
    is( list(get_doi({ doi => 'https://doi.org/10.1234/abc' })), '10.1234/abc', "a DOI with a URL prefix" );
    is( list(get_doi({ doi => 'doi:10.1234/abc' })), '10.1234/abc', "a DOI with 'doi:'" );
    
    is( get_url({ url => 'http://example.org/x' }), 'http://example.org/x', "a URL" );
    is( get_url({ URL => 'http://dx.doi.org/10.1/x' }), undef, "a DOI URL is not returned" );
    
    my %type = ( 'book' => 'book', 'book-chapter' => 'book chapter', 'report' => 'serial monograph',
		 'dissertation' => 'Ph.D. thesis', 'journal-article' => 'journal article',
		 'article' => 'journal article', 'monograph' => 'book' );
    
    foreach my $t ( sort keys %type )
    {
	is( get_publication_type({ type => $t }), $type{$t}, "the publication type of '$t'" );
    }
    
    is( get_publication_type({ type => '', journal => 'J' }), 'journal article', "a record with a journal" );
    is( get_publication_type({ type => 'other' }), undef, "an unknown type" );
};


# parse_authorname splits a name into last and first names, in many forms.

subtest 'parsing names' => sub {
    
    my %names = ( 'Smith, A. B.' => 'Smith | A. B.',
		  'A. B. Smith' => 'Smith | A. B.',
		  'Smith A. B.' => 'Smith | A. B.',
		  'Smith A' => 'Smith | A',
		  'Bob Smith' => 'Smith | Bob',
		  'Bob A. Smith' => 'Smith | Bob A.',
		  'J. van der Berg' => 'van der Berg | J.',
		  'van der Berg, J.' => 'van der Berg | J.',
		  'Smith, Jr., A.' => 'Smith Jr. | A.',
		  'Smith, A., Jr.' => 'Smith Jr. | A.',
		  'Smith' => 'Smith | ' );
    
    foreach my $name ( sort keys %names )
    {
	is( list((parse_authorname($name))[0..1]), $names{$name}, "parsed '$name'" );
    }
    
    is( list(parse_authorname({ family => 'Smith', given => 'Anne', suffix => 'Jr.',
				affiliation => [ { name => 'Uni' } ], ORCID => 'x' })),
	'Smith Jr. | Anne | Uni | x', "a Crossref name" );
    is( list((parse_authorname({ last => 'Smith', first => 'A.' }))[0..1]), 'Smith | A.',
	"a BibJSON name" );
    is( list((parse_authorname({ name => 'Anne Smith' }))[0..1]), 'Anne Smith | undef',
	"a name under 'name' is the last name" );
    is( list(parse_authorname(['Smith', 'A.'])), 'Smith | A.', "an array is returned as it is" );
    is( scalar(() = parse_authorname('')), 0, "an empty name" );
    is( scalar(() = parse_authorname({ })), 0, "an empty hash" );
};


# split_authorlist splits a list of authors into names, keeping each name's initials with
# it.

subtest 'splitting author lists' => sub {
    
    my %lists = ( 'Smith, A. B., Jones, C., and Brown, D.' => 'Smith, A. B. || Jones, C. || Brown, D.',
		  'A. Smith, B. Jones and C. Brown' => 'A. Smith || B. Jones || C. Brown',
		  'A. SMITH, B. JONES AND C. BROWN' => 'A. SMITH || B. JONES || C. BROWN',
		  'Smith, A., Jr., Jones, B.' => 'Smith, A. Jr. || Jones, B.',
		  'Wu, X., Li, Y.' => 'Wu, X. || Li, Y.',
		  '  A.  Smith  ' => 'A. Smith',
		  '1, 2' => '' );
    
    foreach my $l ( sort keys %lists )
    {
	is( join(' || ', split_authorlist($l)), $lists{$l}, "split '$l'" );
    }
};


# get_authorname returns one author's last and first names, and get_authorlist returns all
# of them, from PBDB, Crossref, and BibJSON records.

subtest 'authors of a record' => sub {
    
    is( list((get_authorname(pbdb()))[0..1]), 'Smith | A. B.', "the first PBDB author" );
    is( list((get_authorname(pbdb(), 2))[0..1]), 'Jones | C.', "the second" );
    is( list((get_authorname(pbdb(), 3))[0..1]), 'Brown | D.', "the third, from otherauthors" );
    is( list((get_authorname(pbdb(), 5))[0..1]), ' | ', "an author that is not there" );
    is( list((get_authorname(pbdb(otherauthors => 'D/ Brown'), 3))[0..1]), 'Brown | D.',
	"a slash after an initial is a period" );
    
    my $crossref = { author => [ { given => 'Anne', family => 'Smith' },
				 { name => 'University of Somewhere' },
				 { given => 'Carl', family => 'Jones' } ] };
    
    is( list((get_authorname($crossref, 2))[0..1]), 'Jones | Carl', "an affiliation among the Crossref authors is skipped" );
    is( list((get_authorname({ author => 'A. Smith and B. Jones' }, 2))[0..1]), 'Jones | B.',
	"a name from a list in a string" );
    is( list((get_authorname({ authors => ['A. Smith'] }))[0..1]), 'Smith | A.', "'authors' is used like 'author'" );
    is( list((get_authorname({ }))[0..1]), ' | ', "no authors" );
    
    my $r = { authors => 'A. Smith' };
    
    get_authorname($r);
    
    ok( ! exists $r->{author}, "the record is not changed" );
    
    is( authors(pbdb()), 'Smith | A. B. || Jones | C. || Brown | D. || White | E.', "a PBDB author list" );
    is( authors(pbdb(), 3), 'Brown | D. || White | E.', "starting with the third" );
    is( authors(pbdb(author2last => '', otherauthors => '')), 'Smith | A. B.', "a single author" );
    
    is( authors($crossref), 'Smith | Anne || Jones | Carl', "a Crossref list" );
    is( authors({ author => { family => 'Smith', given => 'A.' } }), 'Smith | A.', "a single name as a hash" );
    is( authors({ author => 'A. Smith, B. Jones, C. Brown' }, 2), 'Jones | B. || Brown | C.',
	"a list in a string, starting with the second" );
    is( authors({ }), '', "no authors" );
};


# title_similarity scores two work titles, allowing for differences in case and accents,
# misspellings, missing words, and extra words at the end.

subtest 'title similarity' => sub {
    
    my $t = 'The fossil record of dinosaurs from the Hell Creek Formation of Montana';
    
    is( list(ReferenceMatch::title_similarity($t, $t)), '100 | 0', "equal titles" );
    is( list(ReferenceMatch::title_similarity($t, uc $t)), '100 | 0', "case is ignored" );
    is( list(ReferenceMatch::title_similarity(NFD('Les dinosaures de la Montagne Noire'),
					      'Les dinosaures de la Montagne Noire'))
	, '100 | 0', "accents are ignored" );
    is( list(ReferenceMatch::title_similarity($t, "$t, with notes")), '100 | 0',
	"a short addition at the end" );
    is( list(ReferenceMatch::title_similarity($t, 'Fossil record dinosaurs from Hell Creek Formation Montana')),
	'100 | 0', "stopwords are ignored" );
    
    my ($s, $c) = ReferenceMatch::title_similarity($t, $t =~ s/dinosaurs/dinosuars/r);
    
    ok( $s >= 80 && $s < 100 && $c > 0, "a misspelling lowers the score a little ($s, $c)" );
    
    ($s, $c) = ReferenceMatch::title_similarity($t, $t =~ s/Hell Creek/Lance/r);
    
    ok( $s > 0 && $s < 90, "a different word lowers it more ($s, $c)" );
    is( list(ReferenceMatch::title_similarity($t, 'Marine snails of the Jurassic of England')), '0 | 100',
	"different titles" );
    is( list(ReferenceMatch::title_similarity($t, '')), '0 | 0', "an empty title" );
    is( list(ReferenceMatch::title_similarity('A', 'B')), '0 | 0', "titles without three letters" );
    is( list(ReferenceMatch::title_similarity('The fossil dinosaurs of Montana',
					      'The fossil dino saurs of Montana')), '100 | 0',
	"words run together in the first title" );
    is( list(ReferenceMatch::title_similarity("Part one, in $t", $t)), '100 | 0',
	"a section heading before the title" );
    
    is( list(ReferenceMatch::title_similarity('Dinosaurs', 'Snails')), '0 | 100',
	"two different one-word titles" );
    
    is( list(ReferenceMatch::title_similarity('The fossil dino saurs of Montana',
					      'The fossil dinosaurs of Montana')), '100 | 0',
	"words run together in the second title" );
    
    ($s, $c) = ReferenceMatch::title_similarity('Part one of the second chapter, in Fossil ' .
						 'record of dinosaurs from Montana',
						 'Fossil record of dinosours from Montana');
    
    ok( $s >= 80, "a section heading and a misspelling ($s, $c)" );
    
    is( join(' ', title_words('The <i>Fossil</i> record of ' . NFD('Pétrified') . ' wood')),
	'fossil record petrified wood', "title_words" );
    is( join(' ', title_words(NFD('Pétrified'), 1)), NFC('pétrified'),
	"title_words keeping the accents returns them in NFC" );
};


# short_similarity scores publication titles and publishers, which are short.

subtest 'short similarity' => sub {
    
    is( list(ReferenceMatch::short_similarity('Journal of Paleontology', 'journal of paleontology')),
	'100 | 0', "equal" );
    is( list(ReferenceMatch::short_similarity('Journal of Paleontology', 'Journal of Paleontology Supplement')),
	'70 | 0', "one is a prefix of the other, with a word more" );
    is( list(ReferenceMatch::short_similarity('Journal of Vertebrate Paleontology',
					      'Journal of Vertebrate Paleontology Supplement')),
	'100 | 0', "a longer prefix" );
    is( list(ReferenceMatch::short_similarity('Journal of Paleontology', 'Paleontology, Journal of')),
	'80 | 0', "the same words in another order" );
    is( list(ReferenceMatch::short_similarity('Journal of Paleontology', 'Palaeontology')), '0 | 100',
	"different" );
    is( list(ReferenceMatch::short_similarity('', 'Palaeontology')), '0 | 0', "an empty title" );
    
    is( list(ReferenceMatch::short_similarity('Paleontology Bulletin',
					      'Bulletin of the Society of Paleontology')),
	'50 | 0', "a short title whose words are a small part of a longer one" );
};


# title_multimatch tries every pair of the titles of two references, with their subtitles.

subtest 'several titles' => sub {
    
    is( list(ReferenceMatch::title_multimatch(['Snails', 'Dinosaurs of Montana'], undef,
					      ['Dinosaurs of Montana'], undef)), '100 | 0',
	"the best pair of titles is found" );
    is( list(ReferenceMatch::title_multimatch(['Dinosaurs of Montana: a new study'], undef,
					      ['Dinosaurs of Montana'], ['A new study'])), '100 | 0',
	"the subtitle of the second record is added" );
    
    is( list(ReferenceMatch::title_multimatch(['Dinosaurs of Montana'], ['A new study'],
					      ['Dinosaurs of Montana: a new study'], undef)),
	'100 | 0', "the subtitle of the first record is added" );
};


# author_similarity scores two author names, allowing for case, accents, misspellings,
# swapped fields, and names in the wrong order.

subtest 'author similarity' => sub {
    
    is( list(author_similarity('Smith', 'A.', 'Smith', 'A.')), '100 | 0', "equal" );
    is( list(author_similarity('SMITH', 'a.', 'smith', 'A.')), '100 | 0', "case is ignored" );
    is( list(author_similarity(NFD('Müller'), 'K.', 'Muller', 'K.')), '100 | 0', "accents are ignored" );
    is( list(author_similarity('Smith', 'A.', 'Smyth', 'A.')), '85 | 15', "one letter different" );
    is( list(author_similarity('Rodriguez', 'A.', 'Rodriquex', 'A.')), '70 | 30',
	"two letters different in a long name" );
    is( list(author_similarity('Smith', 'A.', 'Jones', 'A.')), '0 | 100', "different" );
    is( list(author_similarity('Smith', 'A.', 'Smith', 'B.')), '80 | 20', "different first initials" );
    is( list(author_similarity('Smith', '', 'Smith', 'B.')), '90 | 0', "a missing first initial" );
    is( list(author_similarity('A.', 'Smith', 'Smith', 'A.')), '100 | 0', "swapped fields" );
    is( list(author_similarity('Wei', 'Zhang', 'Zhang', 'Wei')), '100 | 0', "names in the wrong order" );
    is( list(author_similarity('Smith Jones', 'A.', 'Jones', 'A.')), '80 | 0', "an extra word" );
    is( list(author_similarity('Smith jr', 'A.', 'Smith', 'A.')), '100 | 0', "'jr' is ignored" );
    is( list(author_similarity('', 'A.', 'Smith', 'A.')), '0 | 0', "an empty name" );
    
    is( list(author_similarity('Smith Jr', 'A.', 'Smith', 'A.')), '100 | 0', "'Jr' is ignored" );
    
    # match_names finds the best match for a name in a list, and its position.
    
    is( list(ReferenceMatch::match_names('Jones', 'C.', ['Smith', 'A.'], ['Jones', 'C.'])),
	'100 | 0 | 2', "match_names" );
    is( list(ReferenceMatch::match_names('Jones', 'C.', ['Smith', 'A.'], 'C. Jonse')),
	'85 | 15 | 2', "a misspelled name given as a string" );
    is( list(ReferenceMatch::match_names('Jones', 'C.', ['Smith', 'A.'])), '0 | 0 | 0', "no match" );
};


# vol_similarity scores volume and issue, or first and last pages, and pubyr_similarity
# scores years.

subtest 'volumes, pages, and years' => sub {
    
    is( list(ReferenceMatch::vol_similarity(5, 2, 5, 2)), '100 | 0', "the same volume and issue" );
    is( list(ReferenceMatch::vol_similarity(5, 2, 5, 3)), '70 | 30', "a different issue" );
    is( list(ReferenceMatch::vol_similarity(5, 2, 6, 2)), '70 | 30', "a different volume" );
    is( list(ReferenceMatch::vol_similarity(5, 2, 6, 3)), '0 | 100', "both different" );
    is( list(ReferenceMatch::vol_similarity(undef, undef, 5, 2)), '0 | 0', "nothing to compare" );
    
    is( list(ReferenceMatch::pubyr_similarity(2000, 2000)), '100 | 0', "the same year" );
    is( list(ReferenceMatch::pubyr_similarity(2000, 2001)), '70 | 30', "one year apart" );
    is( list(ReferenceMatch::pubyr_similarity(2000, 2003)), '0 | 100', "different years" );
    is( list(ReferenceMatch::pubyr_similarity('1990-2000', 1995)), '100 | 0', "a year in a range" );
    is( list(ReferenceMatch::pubyr_similarity('1990-2000', 2005)), '0 | 100', "a year outside it" );
    is( list(ReferenceMatch::pubyr_similarity('1990-', 2005)), '100 | 0', "an open range" );
    is( list(ReferenceMatch::pubyr_similarity(undef, 2000)), '0 | 0', "a missing year" );
};


# ref_similarity compares a reference with a PBDB record. A record compared with itself is
# a complete match.

subtest 'ref_similarity' => sub {
    
    my $s = ref_similarity(pbdb(), pbdb());
    
    is( "$s->{complete_s} $s->{count_s} $s->{sum_s} $s->{sum_c}", '8 8 800 0',
	"identical records match on every field" );
    
    $s = ref_similarity({ title => ['The dinosaurs of Montana'], 'container-title' => ['Journal of Paleontology'],
			  author => [ { family => 'Smith', given => 'A. B.' }, { family => 'Jones', given => 'C.' } ],
			  'published-print' => { 'date-parts' => [[1999]] }, volume => 12, issue => 3,
			  page => '100-120', publisher => 'Society Press' }, pbdb());
    
    is( "$s->{complete_s} $s->{sum_c}", '8 0', "a Crossref record of the same reference" );
    
    $s = ref_similarity(pbdb(reftitle => 'Marine snails of the Jurassic of England'), pbdb());
    
    is( "$s->{title_s} $s->{title_c}", '0 100', "a different title is a conflict" );
    
    $s = ref_similarity(pbdb(author2last => '', otherauthors => ''), pbdb(author2last => '', otherauthors => ''));
    
    is( "$s->{auth2_s} $s->{auth2_c}", '100 0', "two records with one author" );
    
    $s = ref_similarity(pbdb(pubyr => 1950), pbdb(), { max_c => 50 });
    
    is( "$s->{abort} $s->{sum_c}", '1 100', "too much conflict aborts the comparison" );
    
    # The second author's conflict is limited by the second author's similarity.
    
    my %r = (otherauthors => 'D. Brown');
    
    $s = ref_similarity(pbdb(%r), pbdb(author2last => 'Brown', author2init => 'D.',
				     otherauthors => 'C. Jonse'));
    
    is( "$s->{auth2_s} $s->{auth2_c}", '80 7', "a second author found elsewhere in the list" );
};


# ref_match compares a set of query attributes with a full record. A matching DOI is a
# certain match.

subtest 'ref_match' => sub {
    
    is( ref_match({ doi => '10.1234/ABC' }, { DOI => '10.1234/abc' })->{sum_s}, 500,
	"the same DOI in any case" );
    isnt( ref_match({ doi => '10.1111/aaa' }, { doi => '10.2222/bbb' })->{sum_s}, 500,
	  "different DOIs" );
    
    isnt( ref_match({ doi => 'https://doi.org/10.1111/aaa' },
		    { doi => 'https://doi.org/10.2222/bbb' })->{sum_s}, 500,
	  "different DOIs with a prefix" );
    
    my $m = ref_match({ reftitle => 'dinosaurs Montana', author1last => 'Smith', pubyr => 1999 }, pbdb());
    
    is( "$m->{title_s} $m->{auth1_s} $m->{pubyr_s}", '100 90 100',
	"a few words of the title, the first author without initials, and the year" );
    
    $m = ref_match({ author => 'C. Jones', pubyr => 1999 }, pbdb());
    
    is( $m->{auth1_s}, 100, "an author anywhere in the list" );
    
    $m = ref_match({ author => 'C. Jones', author_is_primary => 1 }, pbdb());
    
    is( $m->{auth1_s}, 0, "but not as the first author, if that is required" );
};


# The counts of the debugging codes can be cleared.

subtest 'counts' => sub {
    
    ok( %ReferenceMatch::COUNT, "the comparisons were counted" );
    
    ReferenceMatch::clear_counts();
    
    ok( ! %ReferenceMatch::COUNT, "clear_counts clears them" );
};
