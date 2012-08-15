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
		SELECT taxon_no, orig_no, taxon_name, taxon_rank, status, 
		       taxon_trees.parent_no, opinion_no
		FROM authorities JOIN taxon_trees USING (orig_no)
			LEFT JOIN opinions USING (opinion_no)
		WHERE taxon_no = t;
	END");

$dbh->do("DROP PROCEDURE IF EXISTS tninfo");
$dbh->do("CREATE PROCEDURE tninfo (t int unsigned )
	BEGIN
		SELECT taxon_no, orig_no, taxon_name, taxon_rank, status, 
		       tn.parent_no, opinion_no
		FROM authorities JOIN tn USING (orig_no)
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

# compute_ancestry ( select )
# 
# Using the seed values in the table 'ancestry_aux', which should all be taxon
# numbers, iteratively add all taxonomic parents to the table until no more
# are found.  This will produce a list of all ancestors of the given taxa
# (including the starting taxa themselves).  If 'select' is 1, then finish by
# selecting the taxon numbers from the table.  If 'select' is 2, then 
# select only the taxon numbers representing parents (not the originals).
# 
# This procedure is used in Taxonomy.pm.
# 
# Note that we start by substituting orig_no values for taxon_no values.  This
# allows the caller to seed the table with taxon_nos that are not necessarily
# original.

$dbh->do("DROP PROCEDURE IF EXISTS compute_ancestry");
$dbh->do("CREATE PROCEDURE compute_ancestry (s int)
	BEGIN
		UPDATE ancestry_aux as s
			JOIN authorities as a on s.orig_no = a.taxon_no
			SET s.orig_no = a.orig_no;
		SET \@gen = 1;
		SET \@cnt = 1;
		WHILE \@cnt > 0 DO
			INSERT IGNORE INTO ancestry_aux select parent_no, \@gen+1
				FROM ancestry_aux as s join taxon_trees as t using (orig_no)
				WHERE gen = \@gen;
			SET \@cnt = ROW_COUNT();
			SET \@gen = \@gen + 1;
		END WHILE;
		IF s = 1 THEN
			SELECT s.orig_no FROM ancestry_aux as s ORDER BY gen DESC;
		ELSEIF s = 2 THEN
			SELECT s.orig_no FROM ancestry_aux as s
			WHERE gen > 1 ORDER BY gen DESC;
		END IF;
	END");
