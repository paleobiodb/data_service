-- 
-- Stored procedures for the Paleobiology Database main api
--

-- compute_attr ( last1, last2, others )
-- 
-- Compute an attribution string using the specified last names.

DELIMITER //

CREATE OR REPLACE FUNCTION compute_attr (last1 varchar(80), last2 varchar(80), others varchar(80))
    RETURNS varchar(80) DETERMINISTIC SQL SECURITY INVOKER
    BEGIN
        DECLARE attr varchar(80);
        
        IF binary(last1) REGEXP ' Jr| III| II' THEN
            SET last1 = REPLACE(last1, ' Jr', '');
            SET last1 = REPLACE(last1, ' III', '');
            SET last1 = REPLACE(last1, ' II', '');
        END IF;
        
        SET last1 = TRIM(trailing ',' from TRIM(trailing '.' from last1));
        
        IF binary(last2) REGEXP ' Jr| III| II' THEN
            SET last2 = REPLACE(last2, ' Jr', '');
            SET last2 = REPLACE(last2, ' III', '');
            SET last2 = REPLACE(last2, ' II', '');
        END IF;
        
        SET last2 = TRIM(trailing ',' from TRIM(trailing '.' from last2));
        
        IF (others <> '' OR last2 LIKE '\%et al%') THEN
            RETURN CONCAT(last1, ' et al.');
        ELSEIF last2 <> '' THEN
            RETURN CONCAT(last1, ' and ', last2);
        ELSE
            RETURN last1;
        END IF;
    END //


-- compute_ancestry ( auth_table, tree_table, taxon_nos )

-- Starting with the taxa specified by the parameter 'taxon_nos', generate a
-- list of the corresponding orig_no values along with all parents up to the
-- top of their taxonomic trees.  These values will be left in the table
-- 'ancestry_scratch', which should be locked from before this call until after
-- the values are no longer needed.

-- The locking requirement is unfortunate, but made necessary by a limitation
-- of MySQL.  Stored procedures cannot access temporary tables more than once
-- in the same procedure.  Thus, 'ancestry_scratch' must be a permanent table,
-- shared among all connections to the database.

-- All three parameters must be strings.  The first two are the names of the
-- authority table and tree table to use, respectively.  The third parameter
-- should be a comma-separated list of taxon_no values.

DROP TABLE IF EXISTS ancestry_scratch //
CREATE TABLE ancestry_scratch (orig_no int unsigned primary key, is_base tinyint unsigned) Engine=MyISAM //

CREATE OR REPLACE PROCEDURE compute_ancestry (auth_table varchar(80), tree_table varchar(80),
                                              taxon_nos varchar(32000), option varchar(80))
    BEGIN
        -- Clear the scratch table
        DELETE FROM ancestry_scratch;
        -- Insert the taxonomic concepts specified by 'taxon_nos'
        SET @stmt1 = CONCAT('INSERT INTO ancestry_scratch SELECT orig_no, 1 FROM ',
                            auth_table, ' WHERE taxon_no in(', taxon_nos, ')');
        PREPARE seed_table FROM @stmt1;
        EXECUTE seed_table;
        -- Select the iteraction statement according to the argument 'option'
        IF option = 'immediate' THEN
            SET @stmt2 = CONCAT('INSERT IGNORE INTO ancestry_scratch SELECT immpar_no, 0 ',
                                  'FROM ancestry_scratch as s JOIN ', tree_table, ' using (orig_no) ',
                                  'WHERE immpar_no > 0');
        ELSE
            SET @stmt2 = CONCAT('INSERT IGNORE INTO ancestry_scratch SELECT senpar_no, 0 ',
                                  'FROM ancestry_scratch as s JOIN ', tree_table, ' using (orig_no) ',
                                  'WHERE senpar_no > 0');
        END IF;
        -- Now iterate adding parents to the table until no more are to be found.
        PREPARE compute_parents FROM @stmt2;
        SET @cnt = 1;
        SET @bound = 1;
        WHILE @cnt > 0 AND @bound < 100 DO
            EXECUTE compute_parents;
            SET @cnt = ROW_COUNT();
            SET @bound = @bound + 1;
        END WHILE;
    END //

-- CREATE OR REPLACE PROCEDURE compute_ancestry_immediate (auth_table varchar(80), tree_table varchar(80), taxon_nos varchar(32000))
--     BEGIN
--         -- Clear the scratch table
--         DELETE FROM ancestry_scratch;
--         -- Insert the taxonomic concepts specified by 'taxon_nos'
--         SET @stmt1 = CONCAT('INSERT INTO ancestry_scratch SELECT orig_no, 1 FROM ',
--                              auth_table, ' WHERE taxon_no in(', taxon_nos, ')');
--         PREPARE seed_table FROM @stmt1;
--         EXECUTE seed_table;
--         -- Now iterate adding parents to the table until no more are to be found.
--         SET @stmt2 = CONCAT('INSERT IGNORE INTO ancestry_scratch SELECT immpar_no, 0 ',
--                              'FROM ancestry_scratch as s JOIN ', tree_table, ' using (orig_no) ',
--                              'WHERE immpar_no > 0');
--         PREPARE compute_parents FROM @stmt2;
--         SET @cnt = 1;
--         SET @bound = 1;
--         WHILE @cnt > 0 AND @bound < 100 DO
--             EXECUTE compute_parents;
--             SET @cnt = ROW_COUNT();
--             SET @bound = @bound + 1;
--             END WHILE;
--     END //

CREATE OR REPLACE PROCEDURE compute_ancestry_2 (tree_table varchar(80), orig_table varchar(80))
    BEGIN
        -- Clear the scratch table
        DELETE FROM ancestry_scratch;
        -- Insert the specified taxon numbers
        SET @stmt1 = CONCAT('INSERT IGNORE INTO ancestry_scratch SELECT orig_no, 1 FROM ', orig_table);
        PREPARE seed_table FROM @stmt1;
        EXECUTE seed_table;
        -- Now iterate adding parents to the table until no more are to be found.
        SET @stmt2 = CONCAT('INSERT IGNORE INTO ancestry_scratch SELECT senpar_no, 0 ',
                             'FROM ancestry_scratch as s JOIN ', tree_table, ' using (orig_no) ',
                             'WHERE senpar_no > 0');
        PREPARE compute_parents FROM @stmt2;
        SET @cnt = 1;
        SET @bound = 1;
        WHILE @cnt > 0 AND @bound < 100 DO
            EXECUTE compute_parents;
            SET @cnt = ROW_COUNT();
            SET @bound = @bound + 1;
        END WHILE;
    END //

DELIMITER ;



