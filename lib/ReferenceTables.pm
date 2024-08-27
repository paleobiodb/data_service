# 
# Paleobiology Database - Reference tables
# 
# This module contains code for estabishing the tables for the new reference system.
# 


package ReferenceTables;

use strict;

use Carp qw(carp croak);
use Scalar::Util qw(blessed);

use TableDefs qw(%TABLE set_table_name);
use CoreTableDefs;



{
    set_table_name(REF_ROLES => 'ref_roles');
    set_table_name(REF_TYPES => 'ref_types');
    set_table_name(REF_SOURCES => 'ref_sources');
    set_table_name(REF_PUBS => 'ref_pubs');
    set_table_name(REF_ENTRIES => 'ref_local');
    set_table_name(REF_PEOPLE => 'ref_people');
    set_table_name(REF_ATTRIB => 'ref_attrib');
    set_table_name(REF_PUB_ATTRIB => 'ref_pubattrib');
    set_table_name(REF_EXTERNAL => 'ref_external');
    set_table_name(REF_TEMPDATA => 'ref_tempdata');
}
    

# new ( dbh, [debug] )
# 
# Create an instance on which to call the following methods.

sub new {
    
    my ($class, $dbh, $flag) = @_;
    
    croak "you must specify a database handle" unless $dbh;
    
    croak "invalid database handle '$dbh'" unless blessed $dbh && 
	ref($dbh) =~ /DBI::/;
    
    my $instance = { dbh => $dbh };
    
    $instance->{debug_mode} = 1 if $flag eq 'DEBUG';
    $instance->{debug_mode} = 1 if $ENV{DEBUG};
    
    return bless $instance, $class;
}


sub dbh {
    
    return $_[0]{dbh};
}

sub debug_mode {
    
    return $_[0]{debug_mode};
}


# establish_tables ( dbh )
# 
# Drop and re-establish the new reference tables. This will be mainly used during development.

sub establish_tables {
    
    my ($self) = @_;
    
    my $dbh = $self->dbh;
    
    # We have to drop all the foreign keys first, because otherwise they prevent the table
    # drops from going through.
    
    # Start with the tables that hold lists of values.
    
    $self->drop_foreign_keys($TABLE{REF_TYPES});
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{REF_TYPES}");
    
    $dbh->do("CREATE TABLE $TABLE{REF_TYPES} (
		reftype_no      tinyint unsigned PRIMARY KEY auto_increment,
		type		varchar(80) not null
		
	      ) charset=utf8 engine=InnoDB");
    
    print "  Created table REF_TYPES as $TABLE{REF_TYPES}\n";
    
    $self->drop_foreign_keys($TABLE{REF_ROLES});
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{REF_ROLES}");
    
    $dbh->do("CREATE TABLE $TABLE{REF_ROLES} (
		refrole_no      tinyint unsigned PRIMARY KEY auto_increment,
		role		varchar(80) not null
		
	      ) charset=utf8 engine=InnoDB");
    
    print "  Created table REF_ROLES as $TABLE{REF_ROLES}\n";
    
    $self->drop_foreign_keys($TABLE{REF_SOURCES});
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{REF_SOURCES}");
    
    $dbh->do("CREATE TABLE $TABLE{REF_SOURCES} (
		refsource_no    tinyint unsigned PRIMARY KEY auto_increment,
		name		varchar(20) not null,
		main_url	varchar(80) null,
		works_base	varchar(80) null
		
	      ) charset=utf8 engine=InnoDB");
    
    print "  Created table REF_SOURCES as $TABLE{REF_SOURCES}\n";
    
    # Then add the publications table, which depends on REF_TYPES.
    
    $self->drop_foreign_keys($TABLE{REF_PUBS});
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{REF_PUBS}");
    
    $dbh->do("CREATE TABLE $TABLE{REF_PUBS} (
		refpub_no       int unsigned PRIMARY KEY auto_increment,
		pub_type        tinyint unsigned not null,
		pubyr           varchar(4) not null default '',
		attribution     varchar(255) not null default '',
		pub_title       varchar(100) not null default '',
		short_title	varchar(40) not null default '',
		publisher       varchar(80) not null default '',
		city		varchar(80) not null default '',
		doi             varchar(255) not null default '',
		isbn            varchar(20) null,
		issn            varchar(20) null,
		system		varchar(255) not null default '',
		workflow        varchar(255) not null default '',
		ts_created      timestamp not null default current_timestamp,
		ts_modified     timestamp not null default current_timestamp,
		ts_checked      timestamp null,
		authorizer_no   int unsigned null,
		enterer_no      int unsigned null,
		modifier_no     int unsigned null,
		
		KEY (pub_title),
		KEY (short_title),
		KEY (publisher),
		KEY (isbn),
		KEY (issn),
		
		FOREIGN KEY `ref_pubs_type` (pub_type) REFERENCES $TABLE{REF_TYPES}
			(reftype_no) on update cascade on delete restrict
		
	      ) charset=utf8 engine=InnoDB");
		
    print "  Created table REF_PUBS as $TABLE{REF_PUBS}\n";
    
    # Then create the people table and the attribution table, which depend on REF_ROLES.
    
    $self->drop_foreign_keys($TABLE{REF_PEOPLE});
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{REF_PEOPLE}");
    
    $dbh->do("CREATE TABLE $TABLE{REF_PEOPLE} (
		refperson_no    int unsigned PRIMARY KEY not null,
		person_no       int unsigned not null,
		last            varchar(80) not null,
		first           varchar(80) not null default '',
		inits           varchar(20) not null default '',
		affiliation     varchar(255) null,
		orcid           varchar(19) null,
		language_other  varchar(20) null,
		last_other      varchar(80) null,
		first_other     varchar(80) null,
		system		varchar(255) null,
		workflow        varchar(255) null,
		
		KEY `name` (last, first),
		KEY (orcid),
		KEY `person_no` (person_no)
		
		# FOREIGN KEY `ref_people_person` (person_no) REFERENCES $TABLE{PERSON} 
		# 	(person_no) on update cascade on delete set null
		
	      ) charset=utf8 engine=InnoDB");
        
    print "  Created table REF_PEOPLE as $TABLE{REF_PEOPLE}\n";
    
    $self->drop_foreign_keys($TABLE{REF_ENTRIES});
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{REF_ENTRIES}");
    
    $dbh->do("CREATE TABLE $TABLE{REF_ENTRIES} (
		reference_no    int unsigned PRIMARY KEY auto_increment,
		merged_into     int unsigned null,
		ref_type        tinyint unsigned not null,
		pubyr           varchar(4) not null default '',
		attribution     varchar(255) not null default '',
		ref_title       varchar(255) not null default '',
		ref_section     varchar(255) not null default '',
		pub_title       varchar(255) not null default '',
		refpub_no       int unsigned null,
		pub_vol         varchar(20) not null default '',
		pub_no          varchar(20) not null default '',
		language        varchar(20) not null default '',
		doi             varchar(255) not null default '',
		basis           tinyint unsigned not null default '0',
		system          varchar(255) not null default '',
		workflow        varchar(255) not null default '',
		ts_created      timestamp,
		ts_modified     timestamp,
		ts_checked      timestamp,
		authorizer_no   int unsigned not null,
		enterer_no      int unsigned not null,
		modifier_no     int unsigned null,
		
		KEY (pubyr),
		KEY (doi),
		FULLTEXT KEY `attribution` (attribution),
		FULLTEXT KEY `ref_title` (ref_title),
		FULLTEXT KEY `pub_title` (pub_title),
		KEY (authorizer_no),
		KEY (enterer_no),
		KEY (modifier_no),
		KEY (ts_modified),
		
		FOREIGN KEY (merged_into) REFERENCES $TABLE{REF_ENTRIES}
			(reference_no) on update cascade on delete restrict,
		
		FOREIGN KEY `refpub_no` (refpub_no) REFERENCES $TABLE{REF_PUBS}
			(refpub_no) on update cascade on delete restrict,
		
		FOREIGN KEY `ref_type` (ref_type) REFERENCES $TABLE{REF_TYPES}
			(reftype_no) on update cascade on delete restrict
		
	      ) charset=utf8 engine=InnoDB");
    
    print "  Created table REF_ENTRIES as $TABLE{REF_ENTRIES}\n";
    
    $self->drop_foreign_keys($TABLE{REF_ATTRIB});
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{REF_ATTRIB}");
    
    $dbh->do("CREATE TABLE $TABLE{REF_ATTRIB} (
		`reference_no`  int unsigned not null,
		`refperson_no`  int unsigned null,
		`indx`          tinyint not null,
		`last`          varchar(80) not null,
		`first`         varchar(80) not null default '',
		`inits`         varchar(20) not null default '',
		`role`          tinyint unsigned not null,
		`workflow`      varchar(255),
		
		UNIQUE KEY `role_index` (reference_no, `role`, `indx`),
		
		FOREIGN KEY `ref_attrib_ref_local` (reference_no) REFERENCES $TABLE{REF_ENTRIES}
			(reference_no) on update cascade on delete cascade,
		
		FOREIGN KEY `ref_attrib_ref_people` (refperson_no) REFERENCES $TABLE{REF_PEOPLE}
			(refperson_no) on update cascade on delete set null,
		
		FOREIGN KEY `ref_attrib_role` (role) REFERENCES $TABLE{REF_ROLES}
			(refrole_no) on update cascade on delete restrict
		
	      ) charset=utf8 engine=InnoDB");
    
    print "  Created table REF_ATTRIB as $TABLE{REF_ATTRIB}\n";
    
    $self->drop_foreign_keys($TABLE{REF_PUB_ATTRIB});
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{REF_PUB_ATTRIB}");
    
    $dbh->do("CREATE TABLE $TABLE{REF_PUB_ATTRIB} (
		`reference_no`  int unsigned not null,
		`refperson_no`  int unsigned null,
		`role`          tinyint unsigned not null,
		`indx`          tinyint unsigned not null,
		`last`          varchar(80) not null,
		`first`         varchar(80) not null default '',
		`inits`         varchar(20) not null default '',
		`workflow`      varchar(255),
		
		UNIQUE KEY `role_index` (reference_no, `role`, `indx`),
		
		FOREIGN KEY `ref_pattrib_ref_local` (reference_no) REFERENCES $TABLE{REF_ENTRIES}
			(reference_no) on update cascade on delete cascade,
		
		FOREIGN KEY `ref_pattrib_ref_people` (refperson_no) REFERENCES $TABLE{REF_PEOPLE}
			(refperson_no) on update cascade on delete set null,
		
		FOREIGN KEY `ref_pattrib_role` (role) REFERENCES $TABLE{REF_ROLES}
			(refrole_no) on update cascade on delete restrict
		
	      ) charset=utf8 engine=InnoDB");
    
    print "  Created table REF_PUB_ATTRIB as $TABLE{REF_PUB_ATTRIB}\n";
    
    $self->drop_foreign_keys($TABLE{REF_EXTERNAL});
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{REF_EXTERNAL}");
    
    $dbh->do("CREATE TABLE $TABLE{REF_EXTERNAL} (
		`refext_no`       int unsigned PRIMARY KEY auto_increment,
		`reference_no`    int unsigned null,
		`source`          tinyint unsigned not null,
		`ts_fetched`      timestamp,
		`ts_checked`	  timestamp,
		`query_text`      text not null default '',
		`query_url`       text not null default '',
		`response_code`   varchar(80) not null default '',
		`response_index`  tinyint,
		`source_data`     mediumtext not null default '',
		`similarity`	  varchar(255) not null default '',
		`attribution`     text not null default '', 
		`pubyr`           varchar(4) not null,
		`ref_type`        tinyint unsigned null,
		`ref_title`       varchar(255) not null default '',
		`ref_subtitle`    varchar(255) not null default '',
		`pub_title`       varchar(255) not null default '',
		`pub_vol`         varchar(20) not null default '',
		`pub_no`          varchar(20) not null default '',
		`doi`             varchar(255) not null default '',
		`system`          varchar(255) not null default '',
		`workflow`        varchar(255) not null default '',
		
		FOREIGN KEY `ref_external_ref_local` (reference_no) REFERENCES $TABLE{REF_ENTRIES}
			(reference_no) on update cascade on delete set null,
		
		FOREIGN KEY `ref_external_source` (source) REFERENCES $TABLE{REF_SOURCES}
			(refsource_no) on update cascade on delete restrict,
		
		FOREIGN KEY `ref_external_type` (ref_type) REFERENCES $TABLE{REF_TYPES}
			(reftype_no) on update cascade on delete restrict
		
	      ) charset=utf8 engine=InnoDB");
    
    print "  Created table REF_EXTERNAL as $TABLE{REF_EXTERNAL}\n";
    
    $dbh->do("DROP TABLE IF EXISTS $TABLE{REF_TEMPDATA}");
    
    $dbh->do("CREATE TABLE $TABLE{REF_TEMPDATA} (
		`reftemp_no`      int unsigned PRIMARY KEY auto_increment,
		`reference_no`    int unsigned null,
		`source`          tinyint unsigned not null,
		`ts_fetched`      timestamp,
		`session_id`      varchar(80),
		`query_text`      text not null default '',
		`query_url`       text not null default '',
		`response_code`   varchar(80) not null default '',
		`response_index`  tinyint unsigned,
		`source_data`     mediumtext not null default '',
		`similarity`      varchar(255) not null default '',
		`attribution`     text not null default '', 
		`pubyr`           varchar(4) not null,
		`ref_type`        tinyint unsigned null,
		`ref_title`       varchar(255) not null default '',
		`ref_subtitle`    varchar(255) not null default '',
		`pub_title`       varchar(255) not null default '',
		`pub_vol`         varchar(20) not null default '',
		`pub_no`          varchar(20) not null default '',
		`doi`             varchar(255) not null default '',
		`workflow`        varchar(255) not null default '',
		
		FOREIGN KEY `ref_tempdata_ref_local` (reference_no) REFERENCES $TABLE{REF_ENTRIES}
			(reference_no) on update cascade on delete set null,
		
		FOREIGN KEY `ref_tempdata_source` (source) REFERENCES $TABLE{REF_SOURCES}
			(refsource_no) on update cascade on delete restrict,
		
		FOREIGN KEY `ref_tempdata_type` (ref_type) REFERENCES $TABLE{REF_TYPES}
			(reftype_no) on update cascade on delete restrict
		
	      ) charset=utf8 engine=InnoDB");
    
    print "  Created table REF_TEMPDATA as $TABLE{REF_TEMPDATA}\n";
}


# drop_foreign_keys ( table_specifier )
# 
# Drop all foreign keys that reference the specified table.

sub drop_foreign_keys {
    
    my ($self, $tablename) = @_;
    
    my $dbh = $self->dbh;
    
    my ($database, $table);
    
    if ( $tablename =~ qr{ ^ `? (.+) `? [.] `? (.+) `? $ }xs )
    {
	$database = $1;
	$table = $2;
    }
    
    elsif ( $tablename =~ qr{ ^ `? ([^.]+) `? $ }xs )
    {
	$database = 'database()';
	$table = "'$1'";
    }
    
    else
    {
	croak "Could not parse table name '$tablename'";
    }
    
    debug_lines('') if $self->debug_mode;
    
    my $sql = "SELECT CONCAT('ALTER TABLE IF EXISTS `', CONSTRAINT_SCHEMA, '`.`', TABLE_NAME, '` 
		DROP FOREIGN KEY IF EXISTS `', CONSTRAINT_NAME, '`')
		FROM information_schema.REFERENTIAL_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA=$database and REFERENCED_TABLE_NAME=$table";
    
    debug_lines($sql,'') if $self->debug_mode;
    
    my $result = $dbh->selectcol_arrayref($sql);
    
    if ( ref $result eq 'ARRAY' && @$result )
    {
	foreach my $stmt ( @$result )
	{
	    debug_lines($sql,'') if $self->debug_mode;
	    $dbh->do($stmt);
	}
    }
}


# fill_from_refs ( dbh )
# 
# Fill the new reference tables using the current contents of REFERENCE_DATA (pbdb.refs)

sub load_from_refs {
    
    my ($self) = @_;
    
    my ($sql, $result);
    
    # Start with the constant value tables.
    
    $self->fill_value_tables;
    
    # Then fill the publications table using distinct publication info.
    
    $self->fill_publication_tables;
    
    # Finally, fill the entries table using data from the old 'refs' table.
}


sub load_values_from_refs {
    
    my ($self) = @_;
    
    my $dbh = $self->dbh;
    
    my ($sql, $result);
    
    print "\nFilling value tables:\n";
    
    $sql = "INSERT INTO $TABLE{REF_TYPES} (type, reftype_no)
	    SELECT DISTINCT publication_type, cast(publication_type as integer) as num
	    FROM $TABLE{REFERENCE_DATA}
	    WHERE publication_type <> '' ORDER BY num asc";
    
    debug_lines('', $sql) if $self->debug_mode;
    
    $result = $dbh->do($sql);
    
    print "  Inserted $result reference types into $TABLE{REF_TYPES}\n";
    
    $sql = "INSERT INTO $TABLE{REF_ROLES} (role, refrole_no)
		VALUES ('author', 1), ('editor', 2), ('translator', 3)";
    
    debug_lines('', $sql) if $self->debug_mode;
    
    $result = $dbh->do($sql);
    
    print "  Inserted $result reference roles into $TABLE{REF_ROLES}\n";
    
    $sql = "INSERT INTO $TABLE{REF_SOURCES} (name, refsource_no, main_url, works_base)
		VALUES
		('crossref', 1, 'https://crossref.org/', 'https://api.crossref.org/works'),
		('xdd', 2, 'https://xdd.wisc.edu/', 'https://xdd.wisc.edu/api/articles')";
    
    debug_lines('', $sql) if $self->debug_mode;
    
    $result = $dbh->do($sql);
    
    print "  Inserted $result reference sources into $TABLE{REF_SOURCES}\n";
}


sub load_publications_from_refs {
    
    my ($self) = @_;
    
    my $dbh = $self->dbh;
    
    my ($sql, $result);
    
    $sql = "INSERT INTO $TABLE{REF_PUBS} (pub_type, pubyr, attribution, pub_title,
		publisher, city, doi, ts_created, ts_modified, 
		authorizer_no, enterer_no, modifier_no)
	    SELECT
		GROUP_CONCAT(DISTINCT pub_type SEPARATOR '|'), 
		if(pub_type in ('journal article', 'serial monograph', 'news article'),
		   NULL, GROUP_CONCAT(DISTINCT pubyr SEPARATOR '|')),
		NULL,
		pubtitle,
		GROUP_CONCAT(DISTINCT publisher SEPARATOR '|'),
		GROUP_CONCAT(DISTINCT city SEPARATOR '|'),
		if(pub_type in ('journal article', 'serial monograph', 'news article'),
		   NULL, GROUP_CONCAT(DISTINCT doi SEPARATOR '|')),
		min(ts_created), max(ts_modified),
		min(if(authorizer_no>0,authorizer_no,null)), 
		min(if(enterer_no>0,enterer_no,null)),
		min(if(modifier_no>0,modifier_no,null))
	    FROM $TABLE{REFERENCE_DATA}
	    WHERE pubtitle <> ''
	    GROUP BY pubtitle";
    
    debug_lines('', $sql) if $self->debug_mode;
    
    $result = $dbh->do($sql);
    
    print "  Inserted $result distinct publications into $TABLE{REF_PUBS}\n";
}


sub debug_lines {
    
    foreach my $line ( @_ )
    {
	print STDERR "$line\n";
    }
}


1;
