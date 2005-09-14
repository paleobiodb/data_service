ALTER TABLE `collections` ADD `authorizer_no` INT( 10 ) UNSIGNED NOT NULL AFTER `modifier` ,ADD `enterer_no` INT( 10 ) UNSIGNED NOT NULL AFTER `authorizer_no` ,ADD `modifier_no` INT( 10 ) UNSIGNED NOT NULL AFTER `enterer_no` ;

ALTER TABLE `occurrences` ADD `authorizer_no` INT( 10 ) UNSIGNED NOT NULL AFTER `modifier` ,ADD `enterer_no` INT( 10 ) UNSIGNED NOT NULL AFTER `authorizer_no` ,ADD `modifier_no` INT( 10 ) UNSIGNED NOT NULL AFTER `enterer_no` ;

ALTER TABLE `reidentifications` ADD `authorizer_no` INT( 10 ) UNSIGNED NOT NULL AFTER `modifier` ,ADD `enterer_no` INT( 10 ) UNSIGNED NOT NULL AFTER `authorizer_no` ,ADD `modifier_no` INT( 10 ) UNSIGNED NOT NULL AFTER `enterer_no` ;

ALTER TABLE `refs` ADD `authorizer_no` INT( 10 ) UNSIGNED NOT NULL AFTER `modifier` ,ADD `enterer_no` INT( 10 ) UNSIGNED NOT NULL AFTER `authorizer_no` ,ADD `modifier_no` INT( 10 ) UNSIGNED NOT NULL AFTER `enterer_no` ;



ALTER TABLE `occurrences` ADD INDEX ( `authorizer_no` ); 
ALTER TABLE `occurrences` ADD INDEX ( `enterer_no` ); 
ALTER TABLE `occurrences` ADD INDEX ( `modifier_no` ); 

ALTER TABLE `reidentifications` ADD INDEX ( `authorizer_no` ); 
ALTER TABLE `reidentifications` ADD INDEX ( `enterer_no` ); 
ALTER TABLE `reidentifications` ADD INDEX ( `modifier_no` ); 
ALTER TABLE `refs` ADD INDEX ( `authorizer_no` ); 
ALTER TABLE `refs` ADD INDEX ( `enterer_no` ); 
ALTER TABLE `refs` ADD INDEX ( `modifier_no` );
ALTER TABLE `collections` ADD INDEX ( `authorizer_no` ); 
ALTER TABLE `collections` ADD INDEX ( `enterer_no` ); 
ALTER TABLE `collections` ADD INDEX ( `modifier_no` ); 
