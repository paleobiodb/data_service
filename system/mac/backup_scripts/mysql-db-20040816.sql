-- MySQL dump 8.22
--
-- Host: localhost    Database: mysql
---------------------------------------------------------
-- Server version	4.0.13-standard

--
-- Current Database: mysql
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ mysql;

USE mysql;

--
-- Table structure for table 'columns_priv'
--

DROP TABLE IF EXISTS columns_priv;
CREATE TABLE columns_priv (
  Host char(60) binary NOT NULL default '',
  Db char(64) binary NOT NULL default '',
  User char(16) binary NOT NULL default '',
  Table_name char(64) binary NOT NULL default '',
  Column_name char(64) binary NOT NULL default '',
  Timestamp timestamp(14) NOT NULL,
  Column_priv set('Select','Insert','Update','References') NOT NULL default '',
  PRIMARY KEY  (Host,Db,User,Table_name,Column_name)
) TYPE=MyISAM COMMENT='Column privileges';

/*!40000 ALTER TABLE columns_priv DISABLE KEYS */;

--
-- Dumping data for table 'columns_priv'
--


LOCK TABLES columns_priv WRITE;
INSERT INTO columns_priv VALUES ('%','pbdb','chronos','person','reversed_name',20040210113319,'Select'),('%','pbdb','chronos','person','name',20040210113319,'Select'),('%','pbdb','chronos','person','person_no',20040210113319,'Select'),('%','pbdb','chronos','person','email',20040210113319,'Select'),('%','pbdb','chronos','person','is_authorizer',20040210113319,'Select'),('%','pbdb','chronos','person','active',20040210113319,'Select'),('%','pbdb','chronos','person','marine_invertebrate',20040210113319,'Select'),('%','pbdb','chronos','person','PACED',20040210113319,'Select'),('%','pbdb','chronos','person','paleobotany',20040210113319,'Select'),('%','pbdb','chronos','person','taphonomy',20040210113319,'Select'),('%','pbdb','chronos','person','vertebrate',20040210113319,'Select'),('%','pbdb','chronos','person','preferences',20040210113319,'Select'),('%','pbdb','chronos','person','created',20040210113319,'Select'),('%','pbdb','chronos','person','modified',20040210113319,'Select'),('%','pbdb','chronos','person','last_action',20040210113319,'Select'),('%','pbdb','chronos','person','superuser',20040210124852,'Select');

/*!40000 ALTER TABLE columns_priv ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table 'db'
--

DROP TABLE IF EXISTS db;
CREATE TABLE db (
  Host char(60) binary NOT NULL default '',
  Db char(64) binary NOT NULL default '',
  User char(16) binary NOT NULL default '',
  Select_priv enum('N','Y') NOT NULL default 'N',
  Insert_priv enum('N','Y') NOT NULL default 'N',
  Update_priv enum('N','Y') NOT NULL default 'N',
  Delete_priv enum('N','Y') NOT NULL default 'N',
  Create_priv enum('N','Y') NOT NULL default 'N',
  Drop_priv enum('N','Y') NOT NULL default 'N',
  Grant_priv enum('N','Y') NOT NULL default 'N',
  References_priv enum('N','Y') NOT NULL default 'N',
  Index_priv enum('N','Y') NOT NULL default 'N',
  Alter_priv enum('N','Y') NOT NULL default 'N',
  Create_tmp_table_priv enum('N','Y') NOT NULL default 'N',
  Lock_tables_priv enum('N','Y') NOT NULL default 'N',
  PRIMARY KEY  (Host,Db,User),
  KEY User (User)
) TYPE=MyISAM COMMENT='Database privileges';

/*!40000 ALTER TABLE db DISABLE KEYS */;

--
-- Dumping data for table 'db'
--


LOCK TABLES db WRITE;
INSERT INTO db VALUES ('localhost','shared','loadshared','Y','Y','N','Y','Y','Y','N','N','N','N','N','N'),('localhost','pbdb','loadshared','Y','N','N','N','N','N','N','N','N','N','N','N'),('%','pbdb','safe','Y','N','N','N','N','N','N','N','N','N','Y','N'),('%','shared','chronos','Y','N','N','N','N','N','N','N','N','N','N','N'),('%.nceas.ucsb.edu','shared','safe','Y','N','N','N','N','N','N','N','N','N','N','N');

/*!40000 ALTER TABLE db ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table 'func'
--

DROP TABLE IF EXISTS func;
CREATE TABLE func (
  name char(64) binary NOT NULL default '',
  ret tinyint(1) NOT NULL default '0',
  dl char(128) NOT NULL default '',
  type enum('function','aggregate') NOT NULL default 'function',
  PRIMARY KEY  (name)
) TYPE=MyISAM COMMENT='User defined functions';

/*!40000 ALTER TABLE func DISABLE KEYS */;

--
-- Dumping data for table 'func'
--


LOCK TABLES func WRITE;

/*!40000 ALTER TABLE func ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table 'host'
--

DROP TABLE IF EXISTS host;
CREATE TABLE host (
  Host char(60) binary NOT NULL default '',
  Db char(64) binary NOT NULL default '',
  Select_priv enum('N','Y') NOT NULL default 'N',
  Insert_priv enum('N','Y') NOT NULL default 'N',
  Update_priv enum('N','Y') NOT NULL default 'N',
  Delete_priv enum('N','Y') NOT NULL default 'N',
  Create_priv enum('N','Y') NOT NULL default 'N',
  Drop_priv enum('N','Y') NOT NULL default 'N',
  Grant_priv enum('N','Y') NOT NULL default 'N',
  References_priv enum('N','Y') NOT NULL default 'N',
  Index_priv enum('N','Y') NOT NULL default 'N',
  Alter_priv enum('N','Y') NOT NULL default 'N',
  Create_tmp_table_priv enum('N','Y') NOT NULL default 'N',
  Lock_tables_priv enum('N','Y') NOT NULL default 'N',
  PRIMARY KEY  (Host,Db)
) TYPE=MyISAM COMMENT='Host privileges;  Merged with database privileges';

/*!40000 ALTER TABLE host DISABLE KEYS */;

--
-- Dumping data for table 'host'
--


LOCK TABLES host WRITE;

/*!40000 ALTER TABLE host ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table 'tables_priv'
--

DROP TABLE IF EXISTS tables_priv;
CREATE TABLE tables_priv (
  Host char(60) binary NOT NULL default '',
  Db char(64) binary NOT NULL default '',
  User char(16) binary NOT NULL default '',
  Table_name char(60) binary NOT NULL default '',
  Grantor char(77) NOT NULL default '',
  Timestamp timestamp(14) NOT NULL,
  Table_priv set('Select','Insert','Update','Delete','Create','Drop','Grant','References','Index','Alter') NOT NULL default '',
  Column_priv set('Select','Insert','Update','References') NOT NULL default '',
  PRIMARY KEY  (Host,Db,User,Table_name),
  KEY Grantor (Grantor)
) TYPE=MyISAM COMMENT='Table privileges';

/*!40000 ALTER TABLE tables_priv DISABLE KEYS */;

--
-- Dumping data for table 'tables_priv'
--


LOCK TABLES tables_priv WRITE;
INSERT INTO tables_priv VALUES ('%','pbdb','chronos','reidentifications','root@localhost',20040210113722,'Select',''),('%','pbdb','chronos','refs','root@localhost',20040210113716,'Select',''),('%','pbdb','chronos','opinions','root@localhost',20040210113711,'Select',''),('%','pbdb','chronos','occurrences','root@localhost',20040210113704,'Select',''),('%','pbdb','chronos','marinepct','root@localhost',20040210113700,'Select',''),('%','pbdb','chronos','intervals','root@localhost',20040210113653,'Select',''),('%','pbdb','chronos','images','root@localhost',20040210113649,'Select',''),('%','pbdb','chronos','fivepct','root@localhost',20040210113644,'Select',''),('%','pbdb','chronos','ecotaph','root@localhost',20040210113637,'Select',''),('%','pbdb','chronos','correlations','root@localhost',20040210113632,'Select',''),('%','pbdb','chronos','collections','root@localhost',20040210113620,'Select',''),('%','pbdb','chronos','authorities','root@localhost',20040210113526,'Select',''),('%','pbdb','chronos','scales','root@localhost',20040210113728,'Select',''),('%','pbdb','chronos','secondary_refs','root@localhost',20040210113732,'Select',''),('%','pbdb','chronos','statistics','root@localhost',20040210113740,'Select',''),('%','pbdb','chronos','person','root@localhost',20040210124946,'','Select');

/*!40000 ALTER TABLE tables_priv ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table 'user'
--

DROP TABLE IF EXISTS user;
CREATE TABLE user (
  Host varchar(60) binary NOT NULL default '',
  User varchar(16) binary NOT NULL default '',
  Password varchar(16) binary NOT NULL default '',
  Select_priv enum('N','Y') NOT NULL default 'N',
  Insert_priv enum('N','Y') NOT NULL default 'N',
  Update_priv enum('N','Y') NOT NULL default 'N',
  Delete_priv enum('N','Y') NOT NULL default 'N',
  Create_priv enum('N','Y') NOT NULL default 'N',
  Drop_priv enum('N','Y') NOT NULL default 'N',
  Reload_priv enum('N','Y') NOT NULL default 'N',
  Shutdown_priv enum('N','Y') NOT NULL default 'N',
  Process_priv enum('N','Y') NOT NULL default 'N',
  File_priv enum('N','Y') NOT NULL default 'N',
  Grant_priv enum('N','Y') NOT NULL default 'N',
  References_priv enum('N','Y') NOT NULL default 'N',
  Index_priv enum('N','Y') NOT NULL default 'N',
  Alter_priv enum('N','Y') NOT NULL default 'N',
  Show_db_priv enum('N','Y') NOT NULL default 'N',
  Super_priv enum('N','Y') NOT NULL default 'N',
  Create_tmp_table_priv enum('N','Y') NOT NULL default 'N',
  Lock_tables_priv enum('N','Y') NOT NULL default 'N',
  Execute_priv enum('N','Y') NOT NULL default 'N',
  Repl_slave_priv enum('N','Y') NOT NULL default 'N',
  Repl_client_priv enum('N','Y') NOT NULL default 'N',
  ssl_type enum('','ANY','X509','SPECIFIED') NOT NULL default '',
  ssl_cipher blob NOT NULL,
  x509_issuer blob NOT NULL,
  x509_subject blob NOT NULL,
  max_questions int(11) unsigned NOT NULL default '0',
  max_updates int(11) unsigned NOT NULL default '0',
  max_connections int(11) unsigned NOT NULL default '0',
  PRIMARY KEY  (Host,User)
) TYPE=MyISAM COMMENT='Users and global privileges';

/*!40000 ALTER TABLE user DISABLE KEYS */;

--
-- Dumping data for table 'user'
--


LOCK TABLES user WRITE;
INSERT INTO user VALUES ('localhost','root','04c76184452de4ec','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','','','','',0,0,0),('localhost','pbdbuser','3b0e362f7feb113c','Y','Y','Y','Y','N','N','N','N','N','N','N','N','N','N','N','N','Y','Y','N','N','N','','','','',0,0,0),('localhost','paleodbbackup','2f69510e16f23502','Y','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','Y','N','N','N','','','','',0,0,0),('localhost','paleosource','2799b7e040db6c5b','Y','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','Y','N','N','N','N','','','','',0,0,0),('localhost','mkosnik','1c2cbefe032b1ad7','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','Y','','','','',0,0,0),('%.chronos.org','chronos','017c80221a7f3c21','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','','','','',0,0,0),('%.geol.iastate.edu','chronos','017c80221a7f3c21','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','','','','',0,0,0),('localhost','safe','1056855d49727de6','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','','','','',0,0,0),('localhost','chronos','017c80221a7f3c21','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','','','','',0,0,0),('%.nceas.ucsb.edu','safe','1056855d49727de6','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','','','','',0,0,0),('localhost','loadshared','6a761e5906da92d7','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','','','','',0,0,0),('%.sdsc.edu','chronos','017c80221a7f3c21','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','','','','',0,0,0);

/*!40000 ALTER TABLE user ENABLE KEYS */;
UNLOCK TABLES;

