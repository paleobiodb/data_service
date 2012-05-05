#!/usr/local/bin/perl

use lib "../cgi-bin";
use DBConnection;


my $dbh = DBConnection::connect();

$dbh->do("DROP PROCEDURE IF EXISTS anyopinion");
$dbh->do("CREATE PROCEDURE anyopinion ( t int unsigned )
	BEGIN
		SELECT * FROM opinions
		WHERE child_no = t or child_spelling_no = t or parent_no = t or parent_spelling_no = t;
	END");

$dbh->do("DROP PROCEDURE IF EXISTS taxoninfo");
$dbh->do("CREATE PROCEDURE taxoninfo (t int unsigned )
	BEGIN
		SELECT taxon_no, taxon_name, taxon_rank, lft, rgt, status, 
		       parent_spelling_no parent, opinion_no
		FROM authorities JOIN taxa_tree_cache USING (taxon_no)
			LEFT JOIN opinions USING (opinion_no)
		WHERE taxon_no = t;
	END");

$dbh->do("DROP PROCEDURE IF EXISTS tninfo");
$dbh->do("CREATE PROCEDURE tninfo (t int unsigned )
	BEGIN
		SELECT taxon_no, taxon_name, taxon_rank, lft, rgt, status, 
		       parent_spelling_no parent, opinion_no
		FROM authorities JOIN tn USING (taxon_no)
			LEFT JOIN opinions USING (opinion_no)
		WHERE taxon_no = t;
	END");

$dbh->do("DROP PROCEDURE IF EXISTS opinions");
$dbh->do("CREATE PROCEDURE opinions (t int unsigned )
	BEGIN
		SELECT * from order_opinions WHERE orig_no = t;
	END");

$dbh->do("DROP PROCEDURE IF EXISTS opinions2");
$dbh->do("CREATE PROCEDURE opinions2 (t int unsigned, u int unsigned)
	BEGIN
		SELECT * from order_opinions WHERE orig_no in (t, u);
	END");

$dbh->do("DROP PROCEDURE IF EXISTS opinioninfo");
$dbh->do("CREATE PROCEDURE opinioninfo (t int unsigned )
	BEGIN
		SELECT * from order_opinions WHERE opinion_no = t;
	END");

$dbh->do("DROP PROCEDURE IF EXISTS bestopinion");
$dbh->do("CREATE PROCEDURE bestopinion (t int unsigned)
	BEGIN
		SELECT * from best_opinions WHERE orig_no = t;
	END");
