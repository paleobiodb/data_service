#!/opt/local/bin/perl

use lib "../cgi-bin";
use DBConnection;


$Constants::DB_USER = 'root';
$Constants::DB_PASSWD = $ARGV[0];

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
		SELECT a.taxon_no, t.orig_no, a.taxon_name, a.taxon_rank, status, t.synonym_no,
		       t.parent_no, opinion_no
		FROM authorities as a JOIN taxon_trees as t on a.taxon_no = t.spelling_no
			JOIN authorities as a2 on a2.orig_no = t.orig_no
			LEFT JOIN opinions USING (opinion_no)
		WHERE a2.taxon_no = t;
	END");

$dbh->do("DROP PROCEDURE IF EXISTS exactinfo");
$dbh->do("CREATE PROCEDURE exactinfo (t int unsigned )
	BEGIN
		SELECT taxon_no, orig_no, taxon_name, taxon_rank, status, t.synonym_no,
		       t.parent_no, opinion_no
		FROM authorities JOIN taxon_trees as t using (orig_no)
			LEFT JOIN opinions USING (opinion_no)
		WHERE taxon_no = t;
	END");

$dbh->do("DROP PROCEDURE IF EXISTS taxonrange");
$dbh->do("CREATE PROCEDURE taxonrange (t int unsigned )
	BEGIN
		SELECT taxon_no, orig_no, taxon_name, taxon_rank, t.lft, t.rgt, t.synonym_no,
		       t.parent_no
		FROM authorities JOIN taxon_trees as t using (orig_no)
			LEFT JOIN opinions USING (opinion_no)
		WHERE taxon_no = t;
	END");

$dbh->do("DROP PROCEDURE IF EXISTS nameinfo");
$dbh->do("CREATE PROCEDURE nameinfo (t varchar(80))
	BEGIN
		IF instr(t, ' ') > 0 THEN
		SELECT taxon_no, orig_no, taxon_name, taxon_rank, status, t.synonym_no,
		       t.parent_no, opinion_no
		FROM authorities JOIN taxon_trees as t using (orig_no)
			LEFT JOIN opinions using (opinion_no)
		WHERE taxon_name like t and taxon_rank in ('subgenus', 'species', 'subspecies');
		ELSE
		SELECT taxon_no, orig_no, taxon_name, taxon_rank, status, t.synonym_no,
		       t.parent_no, opinion_no
		FROM authorities JOIN taxon_trees as t using (orig_no)
			LEFT JOIN opinions using (opinion_no)
		WHERE taxon_name like t and taxon_rank not in ('subgenus', 'species', 'subspecies');
		END IF;
	END");

$dbh->do("DROP PROCEDURE IF EXISTS namerange");
$dbh->do("CREATE PROCEDURE namerange (t varchar(80))
	BEGIN
		IF instr(t, ' ') > 0 THEN
		SELECT taxon_no, orig_no, taxon_name, taxon_rank, t.lft, t.rgt, t.synonym_no,
		       t.parent_no
		FROM authorities JOIN taxon_trees as t using (orig_no)
			LEFT JOIN opinions using (opinion_no)
		WHERE taxon_name like t and taxon_rank in ('subgenus', 'species', 'subspecies');
		ELSE
		SELECT taxon_no, orig_no, taxon_name, taxon_rank, t.lft, t.rgt, t.synonym_no,
		       t.parent_no
		FROM authorities JOIN taxon_trees as t using (orig_no)
			LEFT JOIN opinions using (opinion_no)
		WHERE taxon_name like t and taxon_rank not in ('subgenus', 'species', 'subspecies');
		END IF;
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

# compute_ancestry ( auth_table, tree_table, taxon_nos )
# 
# Starting with the taxa specified by the parameter 'taxon_nos', generate a
# list of the corresponding orig_no values along with all parents up to the
# top of their taxonomic trees.  These values will be left in the table
# 'ancestry_scratch', which should be locked from before this call until after
# the values are no longer needed.
# 
# The locking requirement is unfortunate, but made necessary by a limitation
# of MySQL.  Stored procedures cannot access temporary tables more than once
# in the same procedure.  Thus, 'ancestry_scratch' must be a permanent table,
# shared among all connections to the database.
# 
# All three parameters must be strings.  The first two are the names of the
# authority table and tree table to use, respectively.  The third parameter
# should be a comma-separated list of taxon_no values.

$dbh->do("DROP TABLE IF EXISTS ancestry_scratch");
$dbh->do("CREATE TABLE ancestry_scratch (orig_no int unsigned primary key, is_base tinyint unsigned) Engine=MyISAM");

$dbh->do("DROP PROCEDURE IF EXISTS compute_ancestry");
$dbh->do("CREATE PROCEDURE compute_ancestry (auth_table varchar(80), tree_table varchar(80), taxon_nos varchar(32000))
	BEGIN
		# Clear the scratch table
		DELETE FROM ancestry_scratch;
		# Insert the taxonomic concepts specified by 'taxon_nos'
		SET \@stmt1 = CONCAT('INSERT INTO ancestry_scratch SELECT orig_no, 1 FROM ',
				     auth_table, ' WHERE taxon_no in(', taxon_nos, ')');
		PREPARE seed_table FROM \@stmt1;
		EXECUTE seed_table;
		# Now iterate adding parents to the table until no more are to
		# be found.
		SET \@stmt2 = CONCAT('INSERT IGNORE INTO ancestry_scratch SELECT parent_no, 0 ',
				     'FROM ancestry_scratch as s JOIN ', tree_table, ' using (orig_no) ',
				     'WHERE parent_no > 0');
		PREPARE compute_parents	FROM \@stmt2;
		SET \@cnt = 1;
		SET \@bound = 1;
		WHILE \@cnt > 0 AND \@bound < 100 DO
			EXECUTE compute_parents;
			SET \@cnt = ROW_COUNT();
			SET \@bound = \@bound + 1;
		END WHILE;
	END");



# $dbh->do("DROP PROCEDURE IF EXISTS compute_ancestry");
# $dbh->do("CREATE PROCEDURE compute_ancestry (s int)
# 	BEGIN
# 		UPDATE IGNORE ancestry_aux as s
# 			JOIN authorities as a on s.orig_no = a.taxon_no
# 			SET s.orig_no = a.orig_no;
# 		DELETE IGNORE s FROM ancestry_aux as s LEFT JOIN authorities as a using (orig_no)
# 			WHERE a.orig_no is null;
# 		SET \@gen = 1;
# 		SET \@cnt = s + 1;
# 		WHILE \@cnt > s DO
# 			INSERT IGNORE INTO ancestry_aux select t.parent_no, \@gen+1
# 				FROM ancestry_aux as s JOIN taxon_trees as t using (orig_no)
# 					JOIN authorities as a on a.taxon_no = t.spelling_no
# 				WHERE t.parent_no > 0 and a.taxon_rank <> 'kingdom' and gen = \@gen;
# 			SET \@cnt = ROW_COUNT();
# 			SET \@gen = \@gen + 1;
# 		END WHILE;
# 	END");


# $dbh->do("DROP PROCEDURE IF EXISTS compute_ancestry");
# $dbh->do("CREATE PROCEDURE compute_ancestry (tree_table varchar(80))
# 	BEGIN
# 		# Replace all values in ancestry_aux with corresponding
# 		# orig_no values.
# 		UPDATE IGNORE ancestry_aux as s
# 			JOIN authorities as a on s.orig_no = a.taxon_no
# 			SET s.orig_no = a.orig_no;
# 		# Delete all values from ancestry_aux that have no orig_no
# 		DELETE IGNORE s FROM ancestry_aux as s LEFT JOIN authorities as a using (orig_no)
# 			WHERE a.orig_no is null;
# 		# Determine the starting depth
# 		SET \@stmt1 = CONCAT('SELECT max(depth) FROM ancestry_aux JOIN ', tree_table,
# 				     ' using (orig_no) INTO \@depth');
# 		PREPARE compute_depth FROM \@stmt1;
# 		EXECUTE compute_depth;
# 		SET \@stmt2 = CONCAT('INSERT IGNORE INTO ancestry_aux SELECT t.parent_no ',
# 				     'FROM ancestry_aux as s JOIN ', tree_table, ' as t using (orig_no) ',
# 				     'WHERE t.parent_no > 0 and depth = ?');
# 		PREPARE compute_parents	FROM \@stmt2;
# 		SET \@cnt = 1;
# 		WHILE \@cnt > 0 DO
# 			EXECUTE compute_parents USING \@depth;
# 			SET \@cnt = ROW_COUNT();
# 			SET \@depth = \@depth - 1;
# 		END WHILE;
# 	END");


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

