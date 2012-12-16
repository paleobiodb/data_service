#!/opt/local/bin/perl

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
# selecting the taxon numbers from the table.  If 'select' is 2, then select
# only the taxon numbers representing parents (not the originals).  If
# 'select' is 0, don't select anything.
# 
# This procedure is used in Taxonomy.pm.
# 
# Note that we start by substituting orig_no values for taxon_no values, and
# then deleting everything that isn't an orig_no.  This allows the caller to seed the
# table with taxon_nos that are not necessarily original.

$dbh->do("DROP PROCEDURE IF EXISTS compute_ancestry");
$dbh->do("CREATE PROCEDURE compute_ancestry (s int)
	BEGIN
		UPDATE IGNORE ancestry_aux as s
			JOIN authorities as a on s.orig_no = a.taxon_no
			SET s.orig_no = a.orig_no;
		DELETE IGNORE s FROM ancestry_aux as s LEFT JOIN authorities as a using (orig_no)
			WHERE a.orig_no is null;
		SET \@gen = 1;
		SET \@cnt = s + 1;
		WHILE \@cnt > s DO
			INSERT IGNORE INTO ancestry_aux select t.parent_no, \@gen+1
				FROM ancestry_aux as s JOIN taxon_trees as t using (orig_no)
					JOIN authorities as a on a.taxon_no = t.spelling_no
				WHERE t.parent_no > 0 and a.taxon_rank <> 'kingdom' and gen = \@gen;
			SET \@cnt = ROW_COUNT();
			SET \@gen = \@gen + 1;
		END WHILE;
	END");

# compute_taxon_match ( a, cg, csg, csp )
# 
# This function takes a name from the authorities table followed by a
# candidate taxon name as genus, subgenus, species.  It returns a number from
# 0-30 indicating how well the two match.

$dbh->do("SET GLOBAL log_bin_trust_function_creators = 1");
$dbh->do("DROP FUNCTION IF EXISTS compute_taxon_match");
$dbh->do("CREATE FUNCTION compute_taxon_match (a varchar(80), cg varchar(80), csg varchar(80), csp varchar(80))
	RETURNS int DETERMINISTIC NO SQL
	BEGIN
		DECLARE ag, asg, asp varchar(80) default '';
		# start by splitting up the value of 'a'
		SET ag = substring_index(a, ' ', 1);
		IF a like '%(%' THEN
			SET asg = substring_index(substring_index(a, '(', -1), ')', 1);
			IF a like '%) %' THEN
				SET asp = trim(substring(a, locate(') ', a)+2));
			END IF;
		ELSEIF a like '% %' THEN
			SET asp = trim(substring(a, locate(' ', a)+1));
		END IF;
		# now compare ag to cg, asg to csg, asp to csp
		IF asp <> '' and asp = csp THEN
			IF cg = ag and csg = asg THEN
				RETURN 30;
			ELSEIF cg = ag THEN
				RETURN 28;
			ELSEIF cg = asg THEN
				RETURN 27;
			ELSEIF csg = ag THEN
				RETURN 26;
			ELSEIF csg = asg THEN
				RETURN 25;
			ELSE
				RETURN 0;
			END IF;
		ELSEIF asp <> '' THEN
			RETURN 0;
		ELSEIF asg <> '' THEN
			IF cg = ag and csg = asg THEN
				RETURN 19;
			ELSEIF cg = asg THEN
				RETURN 17;
			ELSEIF csg = ag THEN
				RETURN 16;
			ELSEIF csg = asg THEN
				RETURN 14;
			ELSE
				RETURN 0;
			END IF;
		ELSEIF ag = cg THEN
			RETURN 18;
		ELSEIF ag = csg THEN
			RETURN 15;
		ELSE
			RETURN 0;
		END IF;
	END");

