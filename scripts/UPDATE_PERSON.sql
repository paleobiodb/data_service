ALTER TABLE `person` ADD `first_name` VARCHAR( 30 ) NOT NULL AFTER `reversed_name` ,
ADD `last_name` VARCHAR( 30 ) NOT NULL AFTER `first_name` ,
ADD `homepage` VARCHAR( 80 ) AFTER `email` ;

