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
drop table if exists shared.session_data;

##
##	ONLY COPY PUBLICALLY ACCESSABLE COLLECTION RECORDS TO DATABASE
##
drop table if exists shared.collections;
create table shared.collections
select * from pbdb.collections where access_level='the public' or release_date<now();
alter table shared.collections add primary key (`collection_no`), add index (`reference_no`), add index (`country`), add index(`state`), add index (`authorizer_no`), add index(`enterer_no`), add index(`modifier_no`), add index (`max_interval_no`), add index (`min_interval_no`), add index (`collection_subset`), add index (`lithology1`), add index(`formation`);

##
##	ONLY COPY PUBLICALLY ACCESSABLE COLLECTION RECORDS TO DATABASE
##
drop table if exists shared.secondary_refs;
create table shared.secondary_refs
select pbdb.secondary_refs.* from pbdb.secondary_refs,shared.collections where pbdb.secondary_refs.collection_no=shared.collections.collection_no;
alter table shared.secondary_refs add primary key (`collection_no`,`reference_no`), add index second_ref(`reference_no`);

##
##	ONLY COPY OCCURRENCES OF PUBLICALLY ACCESSABLE COLLECTIONS
##	RECORDS TO DATABASE
##
drop table if exists shared.occurrences;
create table shared.occurrences
select pbdb.occurrences.* from shared.collections, pbdb.occurrences where pbdb.occurrences.collection_no=shared.collections.collection_no;
alter table shared.occurrences add primary key (`occurrence_no`), add index (`collection_no`), add index (`authorizer_no`), add index (`enterer_no`), add index (`modifier_no`), add index (`genus_name`), add index (`subgenus_name`), add index (`species_name`), add index (`taxon_no`);

##
##	ONLY COPY REIDS OF PUBLICALLY ACCESSABLE COLLECTIONS
##	RECORDS TO DATABASE
##
drop table if exists shared.reidentifications;
create table shared.reidentifications
select pbdb.reidentifications.* from pbdb.reidentifications, shared.occurrences where pbdb.reidentifications.occurrence_no=shared.occurrences.occurrence_no;
alter table shared.reidentifications add primary key (`reid_no`), add index (`occurrence_no`), add index (`collection_no`), add index (`authorizer_no`), add index (`enterer_no`), add index(`modifier_no`), add index (`genus_name`), add index (`subgenus_name`), add index (`species_name`), add index (`taxon_no`);

##
##	ONLY COPY RELEVANT FIELDS FROM PERSON TABLE
##
drop table if exists shared.person;
create table shared.person
select pbdb.person.reversed_name, pbdb.person.name, pbdb.person.first_name,pbdb.person.last_name, pbdb.person.person_no, pbdb.person.email, pbdb.person.is_authorizer, pbdb.person.active, pbdb.person.decapod, pbdb.person.divergence, pbdb.person.marine_invertebrate, pbdb.person.PACED, pbdb.person.paleobotany, pbdb.person.taphonomy, pbdb.person.vertebrate ,pbdb.person.created ,pbdb.person.modified from pbdb.person;
alter table shared.person add primary key (`person_no`);

##
##	COPY ALL THE OTHER TABLES...
##
drop table if exists shared.authorities;
create table shared.authorities
select pbdb.authorities.* from pbdb.authorities;
alter table shared.authorities add primary key (`taxon_no`), add index (`reference_no`), add index(`taxon_no`), add index(`type_taxon_no`);

drop table if exists shared.correlations;
create table shared.correlations
select pbdb.correlations.* from pbdb.correlations;
alter table shared.correlations add primary key (`correlation_no`), add index (`scale_no`), add index(`interval_no`);

drop table if exists shared.ecotaph;
create table shared.ecotaph
select pbdb.ecotaph.* from pbdb.ecotaph;
alter table shared.ecotaph add primary key (`ecotaph_no`), add index (`reference_no`), add index(`taxon_no`);

drop table if exists shared.refs;
create table shared.refs
select pbdb.refs.* from pbdb.refs;
alter table shared.refs add primary key(`reference_no`), add index (`author1last`), add index(`pubyr`), add index (`authorizer_no`), add index (`enterer_no`), add index (`modifier_no`);

drop table if exists shared.opinions;
create table shared.opinions
select pbdb.opinions.* from pbdb.opinions;
alter table shared.opinions add primary key (`opinion_no`), add index (`child_no`), add index(`child_spelling_no`), add index(`parent_no`), add index(`parent_spelling_no`), add index (`reference_no`);

drop table if exists shared.intervals;
create table shared.intervals
select pbdb.intervals.* from pbdb.intervals;
alter table shared.intervals add primary key (`interval_no`), add index(`reference_no`), add index(`interval_name`);

drop table if exists shared.images;
create table shared.images
select pbdb.images.* from pbdb.images;
alter table shared.images add primary key (`image_no`), add index(`taxon_no`);

drop table if exists shared.scales;
create table shared.scales
select pbdb.scales.* from pbdb.scales;
alter table shared.scales add primary key (`scale_no`), add index(`scale_name`);

drop table if exists shared.interval_lookup;
create table shared.interval_lookup
select pbdb.interval_lookup.* from pbdb.interval_lookup;
alter table shared.interval_lookup add primary key (`interval_no`), add index(`ten_my_bin`), add index(`stage_no`), add index(`epoch_no`), add index(`period_no`), add index(`subepoch_no`);

drop table if exists shared.taxa_list_cache;
create table shared.taxa_list_cache
select pbdb.taxa_list_cache.* from pbdb.taxa_list_cache;
alter table shared.taxa_list_cache add primary key (`child_no`,`parent_no`), add index(`parent_no`);

drop table if exists shared.taxa_tree_cache;
create table shared.taxa_tree_cache
select pbdb.taxa_tree_cache.* from pbdb.taxa_tree_cache;
alter table shared.taxa_tree_cache add primary key (`taxon_no`), add index(`lft`), add index(`rgt`), add index(`synonym_no`), add index(`spelling_no`);

