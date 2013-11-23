-- MySQL dump 10.13  Distrib 5.1.59, for apple-darwin10.8.0 (i386)
--
-- Host: localhost    Database: pbdb
-- ------------------------------------------------------
-- Server version	5.1.59-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `scale_data`
--

DROP TABLE IF EXISTS `scale_data`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `scale_data` (
  `scale_no` smallint(5) unsigned NOT NULL,
  `scale_name` varchar(80) NOT NULL,
  `levels` tinyint(3) unsigned NOT NULL,
  `base_age` decimal(9,5) DEFAULT NULL,
  `top_age` decimal(9,5) DEFAULT NULL,
  `default_color` varchar(10) DEFAULT NULL,
  `reference_no` int(10) unsigned NOT NULL,
  PRIMARY KEY (`scale_no`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `scale_data`
--

LOCK TABLES `scale_data` WRITE;
/*!40000 ALTER TABLE `scale_data` DISABLE KEYS */;
INSERT INTO `scale_data` VALUES (1,'GSA 2012',5,'4600.00000','0.00000','#000000',0),(100,'10 Million Year Bins (Phanerozoic)',1,'550.00000','0.00000','#000000',0);
/*!40000 ALTER TABLE `scale_data` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `scale_level_data`
--

DROP TABLE IF EXISTS `scale_level_data`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `scale_level_data` (
  `scale_no` smallint(5) unsigned NOT NULL,
  `level` smallint(5) unsigned NOT NULL DEFAULT '0',
  `level_name` varchar(80) NOT NULL,
  `sample` tinyint(1) NOT NULL,
  `reference_no` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`scale_no`,`level`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `scale_level_data`
--

LOCK TABLES `scale_level_data` WRITE;
/*!40000 ALTER TABLE `scale_level_data` DISABLE KEYS */;
INSERT INTO `scale_level_data` VALUES (1,1,'Eon',0,NULL),(1,2,'Era',0,NULL),(1,3,'Period',1,NULL),(1,4,'Epoch',1,NULL),(1,5,'Age',1,NULL),(100,1,'Bins',1,0);
/*!40000 ALTER TABLE `scale_level_data` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2013-11-20 17:09:28
