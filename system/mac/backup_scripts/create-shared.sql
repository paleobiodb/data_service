##
##	loadshared
##	screwthemall
##

use shared;

##
##	THESE TABLES SHOULD NEVER EXIST... REMOVE THEM IF THE APPEAR...
##
drop table if exists shared.statistics;
drop table if exists shared.marinepct;
drop table if exists shared.fivepct;
drop table if exists shared.statistics;
drop table if exists shared.session_data;

##
##	ONLY COPY PUBLICALLY ACCESSABLE COLLECTION RECORDS TO DATABASE
##
drop table if exists shared.collections;
create table shared.collections
select * from pbdb.collections where access_level='the public' or release_date<now();

##
##	ONLY COPY PUBLICALLY ACCESSABLE COLLECTION RECORDS TO DATABASE
##
drop table if exists shared.secondary_refs;
create table shared.secondary_refs
select pbdb.secondary_refs.* from pbdb.secondary_refs,shared.collections where pbdb.secondary_refs.collection_no=shared.collections.collection_no;

##
##	ONLY COPY OCCURRENCES OF PUBLICALLY ACCESSABLE COLLECTIONS
##	RECORDS TO DATABASE
##
drop table if exists shared.occurrences;
create table shared.occurrences
select pbdb.occurrences.* from shared.collections, pbdb.occurrences where pbdb.occurrences.collection_no=shared.collections.collection_no;

##
##	ONLY COPY REIDS OF PUBLICALLY ACCESSABLE COLLECTIONS
##	RECORDS TO DATABASE
##
drop table if exists shared.reidentifications;
create table shared.reidentifications
select pbdb.reidentifications.* from pbdb.reidentifications, shared.occurrences where pbdb.reidentifications.occurrence_no=shared.occurrences.occurrence_no;

##
##	ONLY COPY RELEVANT FIELDS FROM PERSON TABLE
##
drop table if exists shared.person;
create table shared.person
select pbdb.person.reversed_name, pbdb.person.name, pbdb.person.person_no, pbdb.person.email, pbdb.person.is_authorizer, pbdb.person.active, pbdb.person.marine_invertebrate, pbdb.person.PACED, pbdb.person.paleobotany, pbdb.person.taphonomy, pbdb.person.vertebrate ,pbdb.person.created ,pbdb.person.modified from pbdb.person;

##
##	COPY ALL THE OTHER TABLES...
##
drop table if exists shared.authorities;
create table shared.authorities
select pbdb.authorities.* from pbdb.authorities;

drop table if exists shared.correlations;
create table shared.correlations
select pbdb.correlations.* from pbdb.correlations;

drop table if exists shared.ecotaph;
create table shared.ecotaph
select pbdb.ecotaph.* from pbdb.ecotaph;

drop table if exists shared.refs;
create table shared.refs
select pbdb.refs.* from pbdb.refs;

drop table if exists shared.opinions;
create table shared.opinions
select pbdb.opinions.* from pbdb.opinions;

drop table if exists shared.intervals;
create table shared.intervals
select pbdb.intervals.* from pbdb.intervals;

drop table if exists shared.images;
create table shared.images
select pbdb.images.* from pbdb.images;

drop table if exists shared.scales;
create table shared.scales
select pbdb.scales.* from pbdb.scales;