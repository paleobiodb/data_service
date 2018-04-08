# 
# The Paleobiology Database
# 
#   ResourceTables.pm
# 
# Routines for manipulating the educational resource tables, plus table property definitions. This
# module only need be included if the tables themselves need to be created or re-created.
# 

package ResourceTables;

use strict;

use Carp qw(croak);

use CoreFunction qw(new_tables_safe);

use ResourceDefs qw($RESOURCE_QUEUE $RESOURCE_IMAGES
		    $RESOURCE_TAG_NAMES $RESOURCE_TAGS $RESOURCE_ACTIVE);


# establish_tables ( class, dbh, options )
# 
# This class method creates database tables necessary to use this class for testing purposes, or
# replaces the existing ones.

sub establish_tables {
    
    my ($class, $dbh, $options) = @_;
    
    $options ||= { };
    
    # First, create new tables and get them ready. Drop any existing tables under these names.
    
    my ($RESQUEUE_NEW) = 'resqueue_new';
    my ($RESIMAGE_NEW) = 'resimage_new';
    my ($RESACTIVE_NEW) = 'resactive_new';
    my ($RESTAGNAMES_NEW) = 'restagnames_new';
    my ($RESTAGS_NEW) = 'restags_new';
    
    $dbh->do("DROP TABLE IF EXISTS $RESQUEUE_NEW");
    
    $dbh->do("CREATE TABLE $RESQUEUE_NEW (
		eduresource_no int unsigned primary key auto_increment,
		status varchar(10) not null,
		title varchar(255) not null,
		description text not null default '',
		url varchar(200) not null default '',
		is_video boolean not null default 0,
		image varchar(255) null,
		author varchar(80) not null default '',
		submitter varchar(80) not null default '',
		tags varchar(255) not null default '',
		audience varchar(10) not null default '',
		email varchar(80) not null default '',
		affil varchar(80) not null default '',
		orcid varchar(29) not null default '',
		taxa varchar(255) not null default '',
		timespan varchar(255) not null default '',
		topics varchar(255) not null default '',
		authorizer_no int unsigned not null default 0,
		enterer_no int unsigned not null default 0,
		modifier_no int unsigned not null default 0,
		enterer_id varchar(36) null,
		created timestamp not null default current_timestamp,
		modified timestamp not null default current_timestamp
	      ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
    
    $dbh->do("DROP TABLE IF EXISTS $RESIMAGE_NEW");

    $dbh->do("CREATE TABLE $RESIMAGE_NEW (
		eduresource_no int(10) unsigned primary key,
		image_data blob null
	      ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
    
    $dbh->do("DROP TABLE IF EXISTS $RESACTIVE_NEW");

    $dbh->do("CREATE TABLE $RESACTIVE_NEW (
		id int(10) unsigned primary key,
		title varchar(255) not null,
		description text default null,
		url varchar(200) default null,
		is_video boolean not null default 0,
		author varchar(80) default null,
		submitter varchar(80) default null,
		image varchar(255) default null,
		authorizer_no int(10) unsigned not null,
		enterer_no int(10) unsigned not null,
		created timestamp not null default current_timestamp,
		modified timestamp not null default current_timestamp
	      ) ENGINE=InnoDB DEFAULT CHARSET=utf8");

    $dbh->do("DROP TABLE IF EXISTS $RESTAGNAMES_NEW");

    $dbh->do("CREATE TABLE $RESTAGNAMES_NEW (
		id mediumint(8) unsigned not null primary key auto_increment,
		name varchar(100) not null
	      ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
    
    $dbh->do("DROP TABLE IF EXISTS $RESTAGS_NEW");
    
    $dbh->do("CREATE TABLE $RESTAGS_NEW (
		resource_id int(10) unsigned not null,
		tag_id int(10) unsigned not null,
		KEY (resource_id),
		KEY (tag_id)
	      ) ENGINE=InnoDB DEFAULT CHARSET=utf8");
    
    # Now create "backup names" for each of these tables, and attempt to rename each new table to
    # the active name and the active table to the backup name. We do this at the same time, and if
    # any of the backup tables exist then the operation FAILS. This is done ON PURPOSE, to try to
    # keep people from deleting the active data through boneheaded mistakes. If you call this
    # routine, or run a program that calls it, the active tables will be empty and all the data
    # will be in the backup tables where you can copy it over manually to the active ones. If you
    # call this routine twice, or run a program that calls it twice, it will fail the second time
    # and the data will still be in the backup tables. When you have copied over the active data,
    # or verified that you really want to delete it, you can manually drop the backup tables.
    
    my $result = new_tables_safe($dbh, $RESQUEUE_NEW => $RESOURCE_QUEUE,
				       $RESIMAGE_NEW => $RESOURCE_IMAGES,
				       $RESACTIVE_NEW => $RESOURCE_ACTIVE,
				       $RESTAGNAMES_NEW => $RESOURCE_TAG_NAMES,
				       $RESTAGS_NEW => $RESOURCE_TAGS);
    
    return $result;
}


1;

