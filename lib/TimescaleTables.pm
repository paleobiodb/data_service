# 
# The Paleobiology Database
# 
#   TimescaleTables.pm
# 

package TimescaleTables;

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw(%TABLE $INTERVAL_DATA $INTERVAL_MAP $SCALE_MAP $REFERENCES);
use TimescaleDefs;

use CoreFunction qw(activateTables loadSQLFile);
use ConsoleLog qw(logMessage logQuestion);

use base 'Exporter';

our(@EXPORT_OK) = qw(establish_timescale_tables copy_international_timescales set_interval_bounds
		   copy_pbdb_timescales copy_macrostrat_timescales process_one_timescale
		   update_timescale_descriptions model_timescale complete_bound_updates 
		   establish_procedures establish_triggers);

our ($TIMESCALE_FIX) = 'timescale_fix';

# $$$ need to write procedure for defining interavals using bounds and priority

our (%INT_BOUND);
our (%BOUND_BY_AGE);

# Table and file names

our $TIMESCALE_WORK = 'tsw';
our $TS_REFS_WORK = 'tsrw';
our $TS_INTS_WORK = 'tsiw';
our $TS_BOUNDS_WORK = 'tsbw';
our $TS_PERMS_WORK = 'tspw';
our $TS_QUEUE_WORK = 'tsqw';

=head1 NAME

Timescale tables

=head1 SYNOPSIS

This module builds and maintains the tables for storing the definitions of timescales and
timescale intervals and boundaries.

=head2 TABLES

The following tables are maintained by this module:

=over 4

=item timescales

Lists each timescale represented in the database.

=back

=cut

=head1 INTERFACE

In the following documentation, the parameter C<dbi> refers to a DBI database handle.

=cut


# establishTimescaleTables ( dbh, options )
# 
# This function creates the timescale tables, or replaces the existing ones.  The existing ones,
# if any, are renamed to *_bak.

sub establish_timescale_tables {
    
    my ($dbh, $options) = @_;
    
    $options ||= { };
    
    # First create the table 'timescales'.  This stores information about each timescale.
    
    $dbh->do("DROP TABLE IF EXISTS $TIMESCALE_WORK");
    
    $dbh->do("CREATE TABLE $TIMESCALE_WORK (
		timescale_no int unsigned PRIMARY KEY AUTO_INCREMENT,
		pbdb_id int unsigned not null default 0,
		macrostrat_id int unsigned not null default 0,
		timescale_name varchar(80) not null,
		priority tinyint unsigned not null default 1,
		min_age decimal(9,5),
		max_age decimal(9,5),
		source_timescale_no int unsigned not null default 0,
		timescale_type enum('', 'supereon', 'eon', 'era', 'period',
			'superepoch', 'epoch', 'subepoch', 'stage', 'substage', 
			'zone', 'chron', 'multi') not null default '',
		timescale_extent varchar(80) not null default '',
		timescale_taxon varchar(80) not null default '',
		timescale_comments text not null default '',
		has_error boolean not null default 0,
		is_enterable boolean not null default 0,
		is_visible boolean not null default 0,
		admin_lock boolean not null default 0,
		reference_no int unsigned not null default 0,
		authorizer_no int unsigned not null default 0,
		enterer_no int unsigned not null default 0,
		modifier_no int unsigned not null default 0,
		created timestamp default current_timestamp,
		modified timestamp default current_timestamp,
		updated timestamp default current_timestamp on update current_timestamp,
		bounds_updated timestamp default current_timestamp on update current_timestamp,
		is_updated boolean not null default 0,
		key (reference_no),
		key (authorizer_no),
		key (min_age),
		key (max_age, min_age),
		key (is_enterable),
		key (is_visible),
		key (is_updated))");
    
    # The table 'timescale_bounds' defines boundaries between intervals.
    
    $dbh->do("DROP TABLE IF EXISTS $TS_BOUNDS_WORK");
    
    $dbh->do("CREATE TABLE $TS_BOUNDS_WORK (
		bound_no int unsigned PRIMARY KEY AUTO_INCREMENT,
		timescale_no int unsigned not null,
		interval_name varchar(80) not null default '',
		bound_type enum('absolute', 'spike', 'same', 'fraction') not null,
		top_no int unsigned not null default 0,
		base_no int unsigned not null default 0,
		range_no int unsigned not null default 0,
		color_no int unsigned not null default 0,
		age decimal(9,5),
		age_prec tinyint,
		age_error decimal(9,5),
		age_error_prec tinyint,
		fraction decimal(9,5),
		fraction_prec tinyint,
		has_error boolean not null default 0,
		is_modeled boolean not null default 0,
		is_spike boolean not null default 0,
		color varchar(10) not null default '',
		interval_type enum('', 'other', 'supereon', 'eon', 'era', 'period',
			'superepoch', 'epoch', 'subepoch', 'stage', 'substage', 
			'zone', 'chron') not null default '',
		orig_age decimal(9,5),
		pbdb_no int unsigned not null default 0,
		macrostrat_no int unsigned not null default 0,
		updated timestamp default current_timestamp on update current_timestamp,
		age_updated boolean not null default 0,
		is_updated boolean not null default 0,
		key (timescale_no),
		key (base_no),
		key (top_no),
		key (range_no),
		key (color_no),
		key (age),
		key (is_updated))");
    
    # The table 'timescale_ints' associates interval names with unique identifiers.
    
    $dbh->do("DROP TABLE IF EXISTS $TS_INTS_WORK");
    
    $dbh->do("CREATE TABLE $TS_INTS_WORK (
		interval_no int unsigned PRIMARY KEY AUTO_INCREMENT,
		macrostrat_id int unsigned not null default 0,
		interval_name varchar(80) not null,
		interval_type enum('', 'other', 'supereon', 'eon', 'era', 'period',
			'superepoch', 'epoch', 'subepoch', 'stage', 'substage', 
			'zone', 'chron') not null default '',
		abbrev varchar(10) not null default '',
		timescale_no int unsigned not null,
		bound_no int unsigned not null,
		priority tinyint unsigned not null,
		early_age decimal(9,5) not null,
		early_age_prec tinyint,
		late_age decimal(9,5) not null,
		late_age_prec tinyint,
		color varchar(10) not null default '',
		reference_no int unsigned not null default 0,
		orig_early decimal(9,5) null,
		orig_late decimal(9,5) null,
		orig_color varchar(10) null,
		orig_refno int unsigned null,
		macrostrat_color varchar(10) null,
		KEY (macrostrat_id),
		KEY (timescale_no),
		UNIQUE KEY (interval_name))");
       
    activateTables($dbh, $TIMESCALE_WORK => $TABLE{TIMESCALE_DATA},
			 $TS_INTS_WORK => $TABLE{TIMESCALE_INTS},
			 $TS_BOUNDS_WORK => $TABLE{TIMESCALE_BOUNDS});
    
    # Now establish the necessary triggers on the new tables.
    
    establish_triggers($dbh, $options);
    
    # Now make sure the timescale_fix table is properly initialized.
    
    $dbh->do("DROP TABLE IF EXISTS $TIMESCALE_FIX");
    
    $dbh->do("CREATE TABLE $TIMESCALE_FIX (
		timescale_no int unsigned not null,
		interval_name varchar(80) not null,
		bound_type enum('absolute', 'spike') null,
		age decimal(9,5) null,
		age_prec tinyint null,
	        age_error decimal(9,5) null,
		age_error_prec tinyint null,
		primary key (timescale_no, interval_name))");
    
    $dbh->do("REPLACE INTO $TIMESCALE_FIX (timescale_no, interval_name, bound_type, age, age_prec,
			age_error, age_error_prec) VALUES
		(2, 'Holocene', 'spike', '0.0117', 4, null, null),
		(1, 'Calabrian', 'spike', '1.80', 2, null, null),
		(1, 'Gelasian', 'spike', '2.58', 2, null, null),
		(1, 'Piacenzian', 'spike', '3.600', 3, null, null),
		(1, 'Zanclean', 'spike', '5.333', 3, null, null),
		(1, 'Messinian', 'spike', '7.246', 3, null, null),
		(1, 'Tortonian', 'spike', '11.63', 2, null, null),
		(1, 'Serravallian', 'spike', '13.82', 2, null, null),
		(1, 'Aquitanian', 'spike', '23.03', 2, null, null),
		(1, 'Chattian', 'spike', '27.82', 2, null, null),
		(1, 'Rupelian', 'spike', '33.9', 1, null, null),
		(1, 'Bartonian', 'absolute', '41.2', 1, null, null),
		(1, 'Lutetian', 'spike', '47.8', 1, null, null),
		(1, 'Ypresian', 'spike', '56.0', 1, null, null),
		(1, 'Thanetian', 'spike', '59.2', 1, null, null),
		(1, 'Selandian', 'spike', '61.6', 1, null, null),
		(1, 'Danian', 'spike', '66.0', 1, null, null),
		(1, 'Maastrichtian', 'spike', '72.1', 1, '0.2', 1),
		(1, 'Campanian', 'absolute', '83.6', 1, '0.2', 1),
		(1, 'Santonian', 'spike', '86.3', 1, '0.5', 1),
		(1, 'Coniacian', 'absolute', '89.8', 1, '0.3', 1),
		(1, 'Turonian', 'spike', '93.9', 1, null, null),
		(1, 'Cenomanian', 'spike', '100.5', 1, null, null),
		(1, 'Albian', 'spike', '113.0', 1, null, null),
		(1, 'Aptian', 'absolute', '125.0', 1, null, null),
		(1, 'Berriasian', 'absolute', '145.0', 1, null, null),
		(1, 'Tithonian', 'absolute', '152.1', 1, '0.9', 1),
		(1, 'Kimmeridgian', 'absolute', '157.3', 1, '1.0', 1),
		(1, 'Oxfordian', 'absolute', '163.5', 1, '1.0', 1),
		(1, 'Callovian', 'absolute', '166.1', 1, '1.2', 1),
		(1, 'Bathonian', 'spike', '168.3', 1, '1.3', 1),
		(1, 'Bajocian', 'spike', '170.3', 1, '1.4', 1),
		(1, 'Aalenian', 'spike', '174.1', 1, '1.0', 1),
		(1, 'Toarcian', 'spike', '182.7', 1, '0.7', 1),
		(1, 'Pliensbachian', 'spike', '190.8', 1, '1.0', 1),
		(1, 'Sinemurian', 'spike', '199.3', 1, '0.3', 1),
		(1, 'Hettangian', 'spike', '201.3', 1, '0.2', 1),
		(1, 'Norian', 'absolute', '227', 0, null, null),
		(1, 'Carnian', 'spike', '237', 0, null, null),
		(1, 'Ladinian', 'spike', '242', 0, null, null),
		(1, 'Anisian', 'absolute', '247.2', 1, null, null),
		(1, 'Olenekian', 'absolute', '251.2', 0, null, null),
		(1, 'Induan', 'spike', '251.902', 3, '0.024', 3),
		(1, 'Changhsingian', 'spike', '254.14', 2, '0.07', 2),
		(1, 'Wuchiapingian', 'spike', '259.1', 1, '0.5', 1),
		(1, 'Capitanian', 'spike', '265.1', 1, '0.4', 1),
		(1, 'Wordian', 'spike', '268.8', 1, '0.5', 1),
		(1, 'Roadian', 'spike', '272.95', 2, '0.11', 2),
		(1, 'Kungurian', 'absolute', '283.5', 1, '0.6', 1),
		(1, 'Artinskian', 'absolute', '290.1', 1, '0.26', 1),
		(1, 'Sakmarian', 'spike', '293.52', 2, '0.17', 2),
		(1, 'Asselian', 'spike', '298.9', 1, '0.15', 2),
		(1, 'Gzhelian', 'absolute', '303.7', 1, '0.1', 1),
		(1, 'Kasimovian', 'absolute', '307.0', 1, '0.1', 1),
		(1, 'Moscovian', 'absolute', '315.2', 1, '0.2', 1),
		(1, 'Bashkirian', 'spike', '323.2', 1, '0.4', 1),
		(1, 'Serpukhovian', 'absolute', '330.9', 1, '0.2', 1),
		(1, 'Visean', 'spike', '346.7', 1, '0.4', 1),
		(1, 'Tournaisian', 'spike', '358.9', 1, '0.4', 1),
		(1, 'Famennian', 'spike', '372.2', 1, '1.6', 1),
		(1, 'Frasnian', 'spike', '382.7', 1, '1.6', 1),
		(1, 'Givetian', 'spike', '387.7', 1, '0.8', 1),
		(1, 'Eifelian', 'spike', '393.3', 1, '1.2', 1),
		(1, 'Emsian', 'spike', '407.6', 1, '2.6', 1),
		(1, 'Pragian', 'spike', '410.8', 1, '2.8', 1),
		(1, 'Lochkovian', 'spike', '419.2', 1, '3.2', 1),
		(2, 'Pridoli', 'spike', '423.0', 1, '2.3', 1),
		(1, 'Ludfordian', 'spike', '425.6', 1, '0.9', 1),
		(1, 'Gorstian', 'spike', '427.4', 1, '0.5', 1),
		(1, 'Homerian', 'spike', '430.5', 1, '0.7', 1),
		(1, 'Sheinwoodian', 'spike', '433.4', 1, '0.8', 1),
		(1, 'Telychian', 'spike', '438.5', 1, '1.1', 1),
		(1, 'Aeronian', 'spike', '440.8', 1, '1.2', 1),
		(1, 'Rhuddanian', 'spike', '443.8', 1, '1.5', 1),
		(1, 'Hirnantian', 'spike', '445.2', 1, '1.4', 1),
		(1, 'Katian', 'spike', '453.0', 1, '0.7', 1),
		(1, 'Sandbian', 'spike', '458.4', 1, '0.9', 1),
		(1, 'Darriwilian', 'spike', '467.3', 1, '1.1', 1),
		(1, 'Dapingian', 'spike', '470.0', 1, '1.4', 1),
		(1, 'Floian', 'spike', '477.7', 1, '1.4', 1),
		(1, 'Tremadocian', 'spike', '485.4', 1, '1.9', 1),
		(1, 'Stage 10', 'absolute', '489.5', 1, null, null),
		(1, 'Jiangshanian', 'spike', '494', 0, null, null),
		(1, 'Paibian', 'spike', '497', 0, null, null),
		(1, 'Guzhangian', 'spike', '500.5', 1, null, null),
		(1, 'Drumian', 'spike', '504.5', 1, null, null),
		(1, 'Stage 5', 'absolute', '509', 0, null, null),
		(1, 'Stage 4', 'absolute', '514', 0, null, null),
		(1, 'Stage 3', 'absolute', '521', 0, null, null),
		(1, 'Stage 2', 'absolute', '529', 0, null, null),
		(1, 'Fortunian', 'spike', '541.0', 1, '1.0', 1),
		(3, 'Ediacaran', 'spike', '635', 0, null, null)");
}



# copy_international_timescales ( dbh, options )
# 
# Copy into the new tables the old set of timescales and intervals corresponding to the standard
# international timescale.

sub copy_international_timescales {

    my ($dbh, $options) = @_;
    
    $options ||= { };
    
    my $authorizer_no = $options->{authorizer_no} || 0;
    my $auth_quoted = $dbh->quote($authorizer_no);
    my ($sql, $result);

    # Use the appropriate prefix for all database procedure calls.
    
    my $dbstring = '';
    
    if ( $TABLE{TIMESCALE_BOUNDS} =~ /^([^.]+[.])/ )
    {
	$dbstring = $1;
    }
    
    # First establish the international timescales.
    
    $sql = "REPLACE INTO $TABLE{TIMESCALE_DATA} (timescale_no, authorizer_no, enterer_no, timescale_name,
	timescale_type, timescale_extent, priority, is_enterable, is_visible, admin_lock) VALUES
	(1, $auth_quoted, $auth_quoted, 'ICS Stages', 'stage', 'Global', 10, 1, 1, 1),
	(2, $auth_quoted, $auth_quoted, 'ICS Epochs', 'epoch', 'Global', 10, 1, 1, 1),
	(3, $auth_quoted, $auth_quoted, 'ICS Periods', 'period', 'Global', 10, 1, 1, 1),
	(4, $auth_quoted, $auth_quoted, 'ICS Eras', 'era', 'Global', 10, 1, 1, 1),
	(5, $auth_quoted, $auth_quoted, 'ICS Eons', 'eon', 'Global', 10, 1, 1, 1)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    # Then copy the interval data from the old tables for scale_no 1.
    
    $sql = "REPLACE INTO $TABLE{TIMESCALE_INTS} (interval_no, interval_name, abbrev, timescale_no, 
		orig_early, orig_late, color, orig_color, orig_refno)
	SELECT i.interval_no, i.interval_name, i.abbrev, 6 - scale_level, i.early_age, i.late_age, 
		sm.color, sm.color, i.reference_no
	FROM $INTERVAL_DATA as i join $SCALE_MAP as sm using (interval_no)
	WHERE scale_no = 1 and interval_no < 3000
	GROUP BY interval_no";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(1, "Inserted 5 timescales and $result intervals from PBDB");
    
    # For every interval in macrostrat whose name matches one already in the table, overwrite its
    # attributes with the attributes from macrostrat.
    
    $sql = "UPDATE $TABLE{TIMESCALE_INTS} as i join $TABLE{MACROSTRAT_INTERVALS} as msi using (interval_name)
	SET i.orig_early = msi.age_bottom, i.orig_late = msi.age_top,
	    i.macrostrat_id = msi.id,
	    i.orig_color = msi.orig_color, i.macrostrat_color = msi.interval_color";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(1, "Updated $result intervals with data from Macrostrat");
    
    # Then we need to establish the bounds for each timescale. Bound #1 will always be 'the present'.
    
    $sql = "TRUNCATE TABLE $TABLE{TIMESCALE_BOUNDS}";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    # $sql = "	INSERT INTO $TABLE{TIMESCALE_BOUNDS} (timescale_no, authorizer_no, enterer_no, age,
    # 			bound_type, lower_no, interval_no)
    # 		VALUES (4, $auth_quoted, $auth_quoted, 0, 'absolute', 0, 0)";
    
    # print STDERR "$sql\n\n" if $options->{debug};
    
    # $result = $dbh->do($sql);
    
    # Then add all of the bounds for each of the 5 international timescales in turn. Bound number
    # 1 will be the present, with an age of zero.
    
    foreach my $level_no (5,4,3,2,1)
    {
	my $timescale_no = 6 - $level_no;
	
	# All of the international timescales start with a top age of 0.
	
	$sql = "INSERT INTO $TABLE{TIMESCALE_BOUNDS} (timescale_no, bound_type, age) 
		VALUES ($timescale_no, 'absolute', 0)";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$dbh->do($sql);
	
	# Then add the lower bounds of each interval in turn. We have to patch in Holocene and
	# Pridoli, which appear levels 1 and 2.

	my $level_expr = "sm.scale_level = $level_no";

	if ( $level_no == 5 )
	{
	    $level_expr = "(sm.scale_level = 5 or sm.interval_no in (32,59))";
	}
	
	$sql = "INSERT INTO $TABLE{TIMESCALE_BOUNDS} (timescale_no, bound_type, interval_name, age, 
		orig_age, color)
	SELECT $timescale_no as timescale_no, 'absolute' as bound_type, tsi.interval_name,
		tsi.orig_early, tsi.orig_early, if(tsi.macrostrat_color <> '', tsi.macrostrat_color, tsi.orig_color)
	FROM scale_map as sm join $TABLE{TIMESCALE_INTS} as tsi using (interval_no)
	WHERE sm.scale_no = 1 and $level_expr
	ORDER BY tsi.orig_early asc";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$dbh->do($sql);
	
	# Link the newly added bounds together into a single sequence.
	
	$sql = "CALL ${dbstring}link_timescale_bounds($timescale_no);";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$dbh->do($sql);
		
	# Set the precision of each of the newly entered bounds to the
	# position of the last non-zero digit after the decimal. Some of these
	# will have to be adjusted by hand after initialization.
	
	$sql = "UPDATE $TABLE{TIMESCALE_BOUNDS} 
		SET age_prec = length(regexp_substr(age, '(?<=[.])\\\\d*?(?=0*\$)'))
		WHERE timescale_no = $timescale_no";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$dbh->do($sql);
	
	# Then set the min and max age for each timescale.
	
	$sql = "CALL ${dbstring}update_timescale_ages($timescale_no)";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$dbh->do($sql);
    }
    
    # Correct bad interval numbers from PBDB
    
    # $sql = "UPDATE $TABLE{TIMESCALE_BOUNDS}
    # 	    SET lower_no = if(lower_no = 3002, 32, if(lower_no = 3001, 59, lower_no))";
    
    # print STDERR "$sql\n\n" if $options->{debug};
    
    # $result = $dbh->do($sql);
    
    # $sql = "UPDATE $TABLE{TIMESCALE_BOUNDS}
    # 	    SET interval_no = if(interval_no = 3002, 32, if(interval_no = 3001, 59, interval_no))";
    
    # print STDERR "$sql\n\n" if $options->{debug};
    
    # $result = $dbh->do($sql);
    
    # logMessage(2, "    Updated $result bad interval numbers from PBDB") if $result and $result > 0;
    
    # $sql = "DELETE FROM $TABLE{TIMESCALE_INTS} WHERE interval_no > 3000";
    
    # print STDERR "$sql\n\n" if $options->{debug};
    
    # $dbh->do($sql);
    
    # Set the other interval attributes, which are attached to the lower bound
    # record. 
    
    $sql = "UPDATE $TABLE{TIMESCALE_BOUNDS} as tsb join $TABLE{TIMESCALE_DATA} as ts using (timescale_no)
		SET tsb.interval_type = ts.timescale_type
		WHERE timescale_no in (1,2,3,4,5)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(2, "    Initialized the attributes of $result bounds");
    
    # Then we create links between same-age bounds across these different
    # scales. 
    
    my %ages;
    
    foreach my $timescale_no ( 2, 3, 4, 5 )
    {
	foreach my $source_no ( 1 )
	{
	    $sql = "UPDATE $TABLE{TIMESCALE_BOUNDS} as tsb join $TABLE{TIMESCALE_BOUNDS} as source on tsb.age = source.age
		SET tsb.bound_type = 'same', tsb.base_no = source.bound_no
		WHERE tsb.timescale_no = $timescale_no and source.timescale_no = $source_no";
	    
	    print STDERR "$sql\n\n" if $options->{debug};
	    
	    $result = $dbh->do($sql);
	    
	    logMessage(2, "    Linked up $result bounds from timescale $source_no to timescale $timescale_no");
	}
    }
    
    # Save the ages that we just loaded from the source databases
    
    $sql = "UPDATE $TABLE{TIMESCALE_BOUNDS} as tsb SET orig_age = age WHERE timescale_no in (1,2,3,4,5)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $dbh->do($sql);
    
    # Then update the international bounds to their currently accepted values and attributes.
    
    update_international_bounds($dbh, $options);

    # Finally, copy the interval start and end ages to the interval records.

    $sql = "UPDATE $TABLE{TIMESCALE_INTS} as tsi join $TABLE{TIMESCALE_BOUNDS} as tsb using (interval_name)
			join $TABLE{TIMESCALE_BOUNDS} as btp on btp.bound_no = tsb.top_no
		SET tsi.early_age = tsb.age, tsi.early_age_prec = tsb.age_prec,
		    tsi.late_age = btp.age, tsi.late_age_prec = btp.age_prec
		WHERE tsb.timescale_no in (1,2,3,4,5)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(2, "    Initialized the ages of $result intervals");
    
    # Finally, we knit pieces of these together into a single timescale, for demonstration
    # purposes. 
    
    my $test_timescale_no = 10;
    
    $sql = "REPLACE INTO $TABLE{TIMESCALE_DATA} (timescale_no, authorizer_no, timescale_name,
	is_visible, admin_lock, timescale_type, timescale_extent) VALUES
	($test_timescale_no, $auth_quoted, 'Time Ruler', 1, 1,
	 'multi', 'Global')";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $dbh->do($sql);
    
    my @boundaries;
    
    add_timescale_chunk($dbh, \@boundaries, 1);
    add_timescale_chunk($dbh, \@boundaries, 2);
    add_timescale_chunk($dbh, \@boundaries, 3);
    add_timescale_chunk($dbh, \@boundaries, 4);
    add_timescale_chunk($dbh, \@boundaries, 5);
    
    set_timescale_boundaries($dbh, $test_timescale_no, \@boundaries, $options);
    
    $sql = "CALL ${dbstring}link_timescale_bounds($test_timescale_no)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $dbh->do($sql);
    
    $sql = "CALL ${dbstring}update_timescale_ages($test_timescale_no)";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $dbh->do($sql);
    
    # Now check each of these new timescales to make sure there are no gaps. This will also let us
    # set the bottom and top bounds on each timescale.
    
 TIMESCALE:
    foreach my $timescale_no (1..5, $test_timescale_no)
    {
	my @bounds;
	
	# Check the timescale.
	
	my @errors = check_timescale_integrity($dbh, $timescale_no, \@bounds);
	
	# Then report.
	
	my $boundary_count = scalar(@bounds);
	my $early_age = $bounds[0]{age};
	my $late_age = $bounds[-1]{age};
	
	logMessage(1, "");
	logMessage(1, "Timescale $timescale_no: $boundary_count boundaries from $early_age Ma to $late_age Ma");
	
	foreach my $e (@errors)
	{
	    logMessage(2, "    $e");
	}
	
	if ( $options->{verbose} && $options->{verbose} > 2 )
	{
	    logMessage(3, "");
	    
	    foreach my $r (@bounds)
	    {
		my $name = $r->{interval_name} || "TOP";
		my $interval_no = $r->{interval_no};
		
		logMessage(3, sprintf("  %-20s%s", $r->{age}, "$name ($interval_no)"));
	    }
	}
    }
    
    logMessage(1, "done.");
}


# copy_pbdb_timescales ( dbh, options )
# 
# Copy into the new tables the old set of timescales and intervals from the pbdb (other than the
# international ones).

sub copy_pbdb_timescales {

    my ($dbh, $options) = @_;
    
    $options ||= { };
    
    my ($sql, $result);
    
    # First copy the timescales from the PBDB.
    
    $sql = "REPLACE INTO $TABLE{TIMESCALE_DATA} (timescale_no, reference_no, pbdb_id, timescale_name,
		timescale_type, authorizer_no, enterer_no, modifier_no, created, modified)
	SELECT scale_no + 100, reference_no, scale_no, scale_name, null, 
		authorizer_no, enterer_no, modifier_no, created, modified
	FROM scales";
    
    print STDERR "\n$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    # Then get ourselves a list of what we just copied.
    
    $sql = "SELECT scale_no + 100 as timescale_no, scale_no as pbdb_id, scale_name
	FROM scales";
    
    my $timescale_list = $dbh->selectall_arrayref($sql, { Slice => {} });
    my $timescale_count = scalar(@$timescale_list);
    
    logMessage(1, "Inserted $timescale_count timescales from the PBDB");
    
    # Then copy the intervals from the PBDB. But skip the ones from scale 1, which were already
    # copied by 'copy_international_timescales' above.
    
    $sql = "INSERT IGNORE INTO $TABLE{TIMESCALE_INTS} (interval_no, interval_name, abbrev,
		timescale_no, orig_early, orig_late, orig_refno)
	SELECT interval_no, interval_name, abbrev, min(c.scale_no) + 100, early_age, late_age, i.reference_no
	FROM $INTERVAL_DATA as i left join scale_map as sm using (interval_no)
		left join correlations as c using (interval_no)
	WHERE sm.scale_no is null
	GROUP BY interval_no";
    
    print STDERR "\n$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(1, "Inserted $result intervals from the PBDB");
    
    # Now go through the timescales we just copied, one by one.
    
 TIMESCALE:
    foreach my $t ( @$timescale_list )
    {
	next unless $t->{timescale_no} && $t->{pbdb_id};
	
	process_one_timescale($dbh, $t->{timescale_no}, $options);
    }
}


sub copy_macrostrat_timescales {

    my ($dbh, $options) = @_;
    
    $options ||= { };
    
    my $authorizer_no = $options->{authorizer_no} || 0;
    my $auth_quoted = $dbh->quote($authorizer_no);
    
    my ($sql, $result);
    
    # First copy the timescales from Macrostrat. Ignore 1,2,3,13,14, which are the international
    # stages, epochs, etc. and have already been taken care of in 'copy_international_timescales'
    # above.
    
    my $skip_list = "1,2,3,11,13,14";
    
    $sql = "REPLACE INTO $TABLE{TIMESCALE_DATA} (timescale_no, reference_no, macrostrat_id, timescale_name,
		timescale_type, authorizer_no, enterer_no)
	SELECT id + 50, 0, id, timescale, null, $auth_quoted as authorizer_no, $auth_quoted as enterer_no
	FROM $TABLE{MACROSTRAT_SCALES}
	WHERE id not in ($skip_list)";
    
    print STDERR "\n$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    # Then get ourselves a list of what we just copied.
    
    $sql = "SELECT id + 50 as timescale_no, id as macrostrat_id, timescale as timescale_name
	FROM $TABLE{MACROSTRAT_SCALES}
	WHERE id not in ($skip_list)";
    
    my $timescale_list = $dbh->selectall_arrayref($sql, { Slice => {} });
    my $timescale_count = scalar(@$timescale_list);
    
    logMessage(1, "Inserted $timescale_count timescales from Macrostrat");
    
    # Update any intervals from these timescales that are already in the table.
    
    $sql = "UPDATE $TABLE{TIMESCALE_INTS} as i join $TABLE{MACROSTRAT_INTERVALS} as msi using (interval_name)
		join $TABLE{MACROSTRAT_SCALES_INTS} as im on im.interval_id = msi.id
	SET i.macrostrat_id = msi.id, i.macrostrat_color = msi.interval_color
	WHERE im.timescale_id not in ($skip_list)";
    
    print STDERR "\n$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(1, "Updated $result existing intervals using Macrostrat data");
    
    # Add any new intervals that are not already in the table.
    
    $sql = "INSERT INTO $TABLE{TIMESCALE_INTS} (interval_no, macrostrat_id, interval_name, abbrev, timescale_no,
		orig_early, orig_late, orig_color, macrostrat_color)
	SELECT msi.id+2000 as interval_no, msi.id, msi.interval_name, msi.interval_abbrev, min(im.timescale_id) + 50,
		msi.age_bottom, msi.age_top, msi.interval_color, msi.orig_color
	FROM $TABLE{MACROSTRAT_INTERVALS} as msi join $TABLE{MACROSTRAT_SCALES_INTS} as im on im.interval_id = msi.id
		left join $TABLE{TIMESCALE_INTS} as tsi using (interval_name)
	WHERE tsi.interval_name is null and im.timescale_id not in ($skip_list)
	GROUP BY msi.id";
    
    print STDERR "\n$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(1, "Inserted $result intervals from Macrostrat");
    
    $result = $dbh->do($sql);
    
    logMessage(1, "Inserted $result intervals from the PBDB");
    
    # Now go through the timescales we just copied, one by one.
    
 TIMESCALE:
    foreach my $t ( @$timescale_list )
    {
	next unless $t->{timescale_no} && $t->{macrostrat_id};
	
	process_one_timescale($dbh, $t->{timescale_no}, $options);
    }

}


sub process_one_timescale {
    
    my ($dbh, $timescale_no, $options) = @_;
    
    $options ||= { };
    
    my ($sql, $result);
    
    my ($timescale_name, $macrostrat_id, $pbdb_id) = $dbh->selectrow_array("
	SELECT timescale_name, macrostrat_id, pbdb_id FROM $TABLE{TIMESCALE_DATA}
	WHERE timescale_no = $timescale_no");
    
    if ( $pbdb_id )
    {
	logMessage(1, "Processing $timescale_name ($timescale_no from PBDB $pbdb_id)");
    }
    
    elsif ( $macrostrat_id )
    {
	logMessage(1, "Processing $timescale_name ($timescale_no from Macrostrat $macrostrat_id)");
    }
    
    else
    {
	logMessage(1, "Processing $timescale_name ($timescale_no source unknown)");
    }
    
    my $is_chron = $timescale_name =~ /chrons/i;
    
    my $authorizer_no = $options->{authorizer_no} || 0;
    my $auth_quoted = $dbh->quote($authorizer_no);
    
    # Delete any bounds that are already in the table.
    
    $sql = "DELETE FROM $TABLE{TIMESCALE_BOUNDS} WHERE timescale_no = $timescale_no";
    
    print STDERR "\n$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(2, "    deleted $result old bounds") if $result && $result > 0;
    
    # Then get all of the intervals for this timescale, in order from youngest to oldest
    
    if ( $pbdb_id )
    {
	# my $order = $is_chron ? 'i.top_age, i.base_age' : 'i.base_age, i.top_age desc';
	
    	$sql = "SELECT interval_no, i.base_age, i.top_age, c.authorizer_no, 
			ir.eml_interval, ir.interval_name
    		FROM interval_lookup as i join correlations as c using (interval_no)
    			join intervals as ir using (interval_no)
    		WHERE scale_no = $pbdb_id
    		ORDER BY i.top_age, i.base_age, i.interval_no";
	
    	print STDERR "\n$sql\n\n" if $options->{debug};
    }
    
    elsif ( $macrostrat_id )
    {
	# my $order = $is_chron ? 'i.age_top, i.age_bottom' : 'i.age_bottom, i.age_top desc';

    	$sql = "SELECT tsi.interval_no, i.age_bottom as base_age, i.age_top as top_age,
    			0 as authorizer_no, i.interval_name, i.interval_type,
			i.id as macrostrat_no
    		FROM $TABLE{MACROSTRAT_SCALES_INTS} as im
    			join $TABLE{MACROSTRAT_INTERVALS} as i on i.id = im.interval_id
    			join $TABLE{TIMESCALE_INTS} as tsi on tsi.macrostrat_id = im.interval_id
    		WHERE im.timescale_id = $macrostrat_id
    		ORDER BY i.age_top, i.age_bottom, tsi.interval_no";
	
    	print STDERR "\n$sql\n\n" if $options->{debug};
    }
    
    else
    {
    	logMessage(1, "    no source for this timescale");
    	return;
    }
    
    my $interval_list = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    if ( ref $interval_list eq 'ARRAY' && @$interval_list )
    {
    	my $interval_count = scalar(@$interval_list);
    	logMessage(1, "    found $interval_count intervals");
    }
    
    else
    {
    	logMessage(1, "    no intervals found for this timescale");
    	return;
    }
    
    # Now go through the intervals and generate boundaries for them. Usually, the upper boundary
    # of each interval will match the lower boundary of the previous one. However, there may be
    # gaps, or alternative interval names, or overlaps. Each of these cases must be dealt with.
    # The variable $link_bound_no records the last non-alias bound that we encountered, while
    # @pending_bound_nos keeps track of alias bounds whose lower_no values need updating.
    
    my ($last_early, $last_late, $link_bound_no, @pending_bound_nos, $last_interval);
    my (%bound_by_age, %top_bound_count, %base_bound_count);
    
 INTERVAL:
    foreach my $i ( @$interval_list )
    {
	# Since we are loading from tables that do not store precisions for the ages, we must
	# simply use the place of the last non-zero digit after the decimal point as the
	# precision. This will not always be correct, and will have to be updated manually after
	# loading. The calls to get_precision() compute these values.
	
	# If no reference number is given for this interval, we should store a null value so it
	# will default to the reference number for the timescale.
	
	# my $reference_no = $i->{reference_no} || 'NULL';
	# my $authorizer_no = $i->{authorizer_no} || $auth_quoted || '0';
	# my $enterer_no = $i->{enterer_no} || $i->{authorizer_no} || $auth_quoted || '0';
	
	my $top_age = $i->{top_age};
	my $base_age = $i->{base_age};
	
	my $interval_no = $i->{interval_no} || 0;
	my $macrostrat_no = $i->{macrostrat_no} || 0;
	my $interval_name;
	
	if ( my $modifier = $i->{eml_interval} )
	{
	    if ( $modifier eq 'Late/Upper' )
	    {
		$modifier = 'Late';
	    }

	    elsif ( $modifier eq 'Early/Lower' )
	    {
		$modifier = 'Early';
	    }

	    else
	    {
		$modifier =~ s/^([el])/uc($1)/e;
	    }

	    $interval_name = $dbh->quote("$modifier $i->{interval_name}");
	}

	else
	{
	    $interval_name = $dbh->quote($i->{interval_name});
	}

	my $interval_type = $i->{interval_type} ? $dbh->quote($i->{interval_type}) : "''";
	
	$top_bound_count{$top_age}++;
	$base_bound_count{$base_age}++;
	
	# If the base age and top age are the same, then skip this interval.
	
	if ( $base_age == $top_age )
	{
	    next INTERVAL;
	}
	
	# Otherwise, we need to find or create bounds for both the top and bottom of the interval.
	
	my $top_prec = get_precision($top_age);
	my $base_prec = get_precision($base_age);
	my $top_bound_no;
	
	# If we have already created a bound in this timescale for the top age, use
	# that. Otherwise, create one. As this bound is not the bottom of any interval in this
	# timescale, we set the interval_name to ''.
	
	unless ( $top_bound_no = $bound_by_age{$top_age} )
	{
	    $sql = "
		INSERT INTO $TABLE{TIMESCALE_BOUNDS} (timescale_no, bound_type, top_no,
			interval_name, age, age_prec, orig_age, pbdb_no, macrostrat_no)
		VALUES ($timescale_no, 'absolute', 0, '', $top_age, $top_prec, 
			$top_age, $interval_no, $macrostrat_no)";
	    
	    print STDERR "\n$sql\n\n" if $options->{debug};
	    
	    $result = $dbh->do($sql);
	    
	    $top_bound_no = $dbh->last_insert_id(undef, undef, undef, undef);
	    $bound_by_age{$top_age} = $top_bound_no;
	}
	
	# Now create a bound for the bottom age, using top_no to link it to the top bound.
	
	$sql = "
		INSERT INTO $TABLE{TIMESCALE_BOUNDS} (timescale_no, bound_type, top_no,
			interval_name, age, age_prec, interval_type, orig_age, 
			pbdb_no, macrostrat_no)
		VALUES ($timescale_no, 'absolute', $top_bound_no, $interval_name, 
			$base_age, $base_prec, $interval_type, $base_age, 
			$interval_no, $macrostrat_no)";
	
	print STDERR "\n$sql\n\n" if $options->{debug};
	
	$result = $dbh->do($sql);
	
	my $base_bound_no = $dbh->last_insert_id(undef, undef, undef, undef);
	$bound_by_age{$base_age} ||= $base_bound_no;
    }

    # Now check to see if multiple intervals hit at least one bound. If so, then this timescale
    # contains at least one overlap. (It is, I suppose, possible that the timescale contains
    # overlaps none of which actually end at the same bounds, but this does not occur in
    # practice.)

    my ($overlap_count);

    foreach my $k ( keys %top_bound_count )
    {
	$overlap_count++ if $top_bound_count{$k} && $top_bound_count{$k} > 1;
    }
    
    foreach my $k ( keys %top_bound_count )
    {
	$overlap_count++ if $top_bound_count{$k} && $top_bound_count{$k} > 1;
    }

    if ( $overlap_count )
    {
	logMessage(2, "    This timescale has at least $overlap_count overlaps");
    }

    # Now model this timescale against the international intervals.

    model_one_timescale($dbh, $timescale_no, { silent => 1, debug => $options->{debug} });
    
    my $a = 1; # we can stop here when debugging
}


# get_precision ( value )
#
# Return the number of digits after the decimal point other than trailing zeros.

sub get_precision {
    
    my ($value) = @_;

    if ( $value =~ qr{ (?<=[.]) (\d*?) (?=0*$) }xs )
    {
	return length($1);
    }

    else
    {
	return 'NULL';
    }
}

# add_timescale_chunk ( timescale_dest, timescale_source, last_boundary_age )
# 
# Add boundaries to the destination timescale, which refer to boundaries in the source timescale.
# Add only boundaries earlier than $last_boundary_age, and return the age of the last boundary
# added.
# 
# This routine is meant to be used in sequence to knit together a timescale with chunks from a
# variety of source timescales. It should be called most recent -> least recent.

sub add_timescale_chunk {

    my ($dbh, $boundary_list, $source_no, $max_age, $late_age) = @_;
    
    my ($sql, $result);
    
    # First get a list of boundaries from the specified timescale, restricted according to the
    # specified bounds.
    
    my $source_quoted = $dbh->quote($source_no);
    my @filters = "tsb.timescale_no = $source_quoted";
    
    if ( $max_age )
    {
	my $quoted = $dbh->quote($max_age);
	push @filters, "age <= $quoted";
    }
    
    if ( @$boundary_list && $boundary_list->[-1]{age} )
    {
	$late_age = $boundary_list->[-1]{age} + 0.1 if ! defined $late_age || $boundary_list->[-1]{age} >= $late_age;
    }
    
    if ( $late_age )
    {
	my $quoted = $dbh->quote($late_age);
	push @filters, "age >= $quoted";
    }
    
    my $filter = "";
    
    if ( @filters )
    {
	$filter = "WHERE " . join( ' and ', @filters );
    }
    
    $sql = "SELECT tsb.bound_no, age, age_prec, age_error, age_error_prec, interval_no, interval_name, is_spike,
		timescale_type, timescale_extent
	    FROM $TABLE{TIMESCALE_BOUNDS} as tsb left join $TABLE{TIMESCALE_INTS} as tsi using (interval_name) 
		join $TABLE{TIMESCALE_DATA} as ts on ts.timescale_no = tsb.timescale_no
	    $filter ORDER BY age asc";
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
    my @results;
    
    @results = @$result if ref $result eq 'ARRAY';
    
    # If we have no results, do nothing.
    
    return unless @results;
    
    # If the top boundary has no interval_no, and the list to which we are adding has at least one
    # member, then remove it.
    
    if ( @$boundary_list && ! $results[0]{interval_no} )
    {
	shift @results;
    }
    
    # Now tie the two ranges together.
    
    # if ( @$boundary_list )
    # {
    # 	$boundary_list->[-1]{lower_no} = $results[0]{interval_no};
    # }
    
    # Alter each record so that it is indicated as a copy of the specified bound.
    
    foreach my $b (@results)
    {
	$b->{bound_type} = 'same';
    }
    
    # Then add the new results on to the list.
    
    push @$boundary_list, @results;
}


# set_timescale_boundaries ( dbh, timescale_no, boundary_list )
# 
# 

sub set_timescale_boundaries {
    
    my ($dbh, $timescale_no, $boundary_list, $options) = @_;
    
    my $result;
    my $sql = "INSERT INTO $TABLE{TIMESCALE_BOUNDS} (timescale_no, bound_type, interval_name, base_no,
		age, age_prec, age_error, age_error_prec, is_spike, interval_type) VALUES\n";
    
    my @values;
    
    my $ts_quoted = $dbh->quote($timescale_no);
    # my $auth_quoted = $dbh->quote($authorizer_no);
    
    foreach my $b (@$boundary_list)
    {
	my $interval_quoted = $dbh->quote($b->{interval_name});
	my $base_quoted = $dbh->quote($b->{base_no} // $b->{bound_no});
	my $age_quoted = $dbh->quote($b->{age});
	my $age_prec_quoted = $dbh->quote($b->{age_prec});
	my $age_error_quoted = $dbh->quote($b->{age_error});
	my $age_error_prec_quoted = $dbh->quote($b->{age_error_prec});
	my $spike_quoted = $dbh->quote($b->{is_spike});
	my $type_quoted = $dbh->quote($b->{timescale_type} // '');
	
	push @values, "($ts_quoted, 'same', $interval_quoted, $base_quoted, " .
	    "$age_quoted, $age_prec_quoted, $age_error_quoted, $age_error_prec_quoted, $spike_quoted, $type_quoted)";
    }
    
    $sql .= join( ",\n" , @values );
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
}


# check_timescale_integrity ( dbh, timescale_no )
# 
# Check the specified timescale for integrity.  If any errors are found, return a list of them.

sub check_timescale_integrity {
    
    my ($dbh, $timescale_no, $bounds_ref, $options) = @_;
    
    my ($sql);
    
    $sql = "	SELECT bound_no, age, interval_name
		FROM $TABLE{TIMESCALE_BOUNDS} as tsb
		WHERE timescale_no = $timescale_no
		ORDER BY age asc";
    
    my ($results) = $dbh->selectall_arrayref($sql, { Slice => { } });
    my (@results);
    
    @results = @$results if ref $results eq 'ARRAY';
    
    my (@errors);
    
    # Make sure that we actually have some results.
    
    unless ( @results )
    {
	push @errors, "No boundaries found";
	return @errors;
    }
    
    # Make sure that the first and last intervals have the correct properties.
    
    if ( $results[0]{interval_name} )
    {
	my $bound_no = $results[0]{bound_no};
	push @errors, "Error in bound $bound_no: should be upper boundary but has interval_no = $results[0]{interval_no}";
    }
    
    # if ( $results[-1]{lower_no} )
    # {
    # 	my $bound_no = $results[-1]{bound_no};
    # 	push @errors, "Error in bound $bound_no: should be lower boundary but has lower_no = $results[-1]{lower_no}";
    # }
    
    # Then check all of the boundaries in sequence.
    
    my ($early_age, $late_age, $last_age, $last_lower_no);
    my $boundary_count = 0;
    
    $results[-1]{last_record} = 1;
    
    # foreach my $r (@results)
    # {
    # 	my $bound_no = $r->{bound_no};
    # 	my $age = $r->{age};
    # 	my $interval_no = $r->{interval_no};
    # 	my $lower_no = $r->{lower_no};
	
    # 	$boundary_count++;
	
    # 	# The first age will be the late end of the scale, the last age will be the early end.
	
    # 	# $late_age //= $age;
    # 	# $early_age = $age;
	
    # 	# Make sure the ages are all defined and monotonic.
	
    # 	unless ( defined $age )
    # 	{
    # 	    push @errors, "Error in bound $bound_no: age is not defined";
    # 	}
	
    # 	if ( defined $last_age && $last_age >= $age )
    # 	{
    # 	    push @errors, "Error in bound $bound_no: age ($age) >= last age ($last_age)";
    # 	}
	
    # 	# Make sure that the interval_no matches the lower_no of the previous
    # 	# record.
	
    # 	if ( defined $last_lower_no )
    # 	{
    # 	    unless ( $interval_no )
    # 	    {
    # 		push @errors, "Error in bound $bound_no: interval_no not defined";
    # 	    }
	    
    # 	    elsif ( $interval_no ne $last_lower_no )
    # 	    {
    # 		push @errors, "Error in bound $bound_no: interval_no ($interval_no) does not match upward ($last_lower_no)";
    # 	    }
    # 	}
	
    # 	$last_lower_no = $lower_no;
	
    # 	unless ( $lower_no || $r->{last_record} )
    # 	{
    # 	    push @errors, "Error in bound $bound_no: lower_no not defined";
    # 	}
    # }
    
    if ( ref $bounds_ref eq 'ARRAY' )
    {
	@$bounds_ref = @results;
    }
    
    return @errors;
}


# update_timescale_descriptions ( dbh, timescale_no, options )
# 
# Update the fields 'timescale_type', 'timescale_extent', and 'timescale_taxon' based on the name
# of each timescale. If $timescale_no is given, just update that timescale. Otherwise, update
# everything.

sub update_timescale_descriptions {
    
    my ($dbh, $timescale_no, $options) = @_;
    
    $options ||= { };
    
    logMessage(1, "Setting timescale descriptions...");
    
    my ($sql, $result);
    
    my $selector = '';
    
    $selector = "and timescale_no = $timescale_no" if $timescale_no && $timescale_no =~ qr{^\d+$} && $timescale_no > 0;
    
    # First clear all existing attributes and re-compute them.
    
    $dbh->do("UPDATE $TABLE{TIMESCALE_DATA}
		SET timescale_type = null, timescale_extent = null, timescale_taxon = null
		WHERE timescale_no > 10 $selector");
    
    # Scan for all of the possible types.
    
    foreach my $type ('stage', 'substage', 'zone', 'chron', 'epoch', 'period', 'era', 'eon')
    {
	my $regex = $type;
	$regex = 's?t?age' if $type eq 'stage';
	$regex = '(?:subs?t?age|unit)' if $type eq 'substage';
	$regex = '(?:zone|zonation)' if $type eq 'zone';
	$regex = 'chron' if $type eq 'chron';
	
	$sql = "UPDATE $TABLE{TIMESCALE_DATA}
		SET timescale_type = '$type'
		WHERE timescale_name rlike '\\\\b${regex}s?\\\\b' $selector";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$result = $dbh->do($sql) + 0;
	
	logMessage(2, "    type = '$type' - $result");
    }
    
    # Scan for taxa that are known to be used in naming zones
    
    foreach my $taxon ('ammonite', 'conodont', 'mammal', 'faunal')
    {
	my $regex = $taxon;
	
	$sql = "UPDATE $TABLE{TIMESCALE_DATA}
		SET timescale_taxon = '$taxon'
		WHERE timescale_name rlike '$regex' $selector";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$result = $dbh->do($sql) + 0;
	
	logMessage(2, "    taxon = '$taxon' - $result");
    }
    
    # Now scan for geographic extents
    
    my (%extent) = ('china' => 'Chinese',
		    'asia' => 'Asian',
		    'north america' => 'North American',
		    'south america' => 'South American',
		    'africa' => 'African',
		    'europe' => 'European',
		    'laurentia' => 'Laurentian',
		    'western interior' => 'Western interior',
		    'boreal' => 'Boreal');
    
    foreach my $regex ( keys %extent )
    {
	my $extent = $extent{$regex};
	
	$sql = "UPDATE $TABLE{TIMESCALE_DATA}
		SET timescale_extent = '$extent'
		WHERE timescale_name rlike '$regex' $selector";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$result = $dbh->do($sql);
	
	logMessage(2, "    extent = '$extent' - $result") if $result && $result > 0;
    }
    
    # Now copy these to the boundaries and intervals.
    
    $sql = "UPDATE $TABLE{TIMESCALE_BOUNDS} as tsb join $TABLE{TIMESCALE_DATA} as ts using (timescale_no)
		SET tsb.interval_type = ts.timescale_type
		WHERE tsb.interval_type = '' or tsb.interval_type is null";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(2, "Updated attributes of $result boundaries to match their timescales") if $result && $result > 0;
    
    $sql = "UPDATE $TABLE{TIMESCALE_INTS} as tsi join $TABLE{TIMESCALE_DATA} as ts using (timescale_no)
		SET tsi.interval_type = ts.timescale_type
		WHERE tsi.interval_type = '' or tsi.interval_type is null";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    $result = $dbh->do($sql);
    
    logMessage(2, "Updated attributes of $result intervals to match their timescales") if $result && $result > 0;    
}


# set_interval_bounds ( dbh, options )
# 
# Update the interval records to note the corresponding bound id.

sub set_interval_bounds {

    my ($dbh, $options) = @_;
    
    my $sql = "
		UPDATE $TABLE{TIMESCALE_INTS} as tsi 
		    join $TABLE{TIMESCALE_BOUNDS} as tsb using (interval_name, timescale_no)
		    join $TABLE{TIMESCALE_BOUNDS} as upper on upper.bound_no = tsb.top_no
		    join $TABLE{TIMESCALE_DATA} as ts on ts.timescale_no = tsb.timescale_no
		SET tsi.bound_no = tsb.bound_no,
		    tsi.timescale_no = tsb.timescale_no, 
		    tsi.priority = ts.priority,
		    tsi.color = tsb.color,
		    tsi.early_age = tsb.age, tsi.early_age_prec = tsb.age_prec,
		    tsi.late_age = upper.age, tsi.late_age_prec = upper.age_prec";

    print STDERR "$sql\n\n" if $options->{debug};
    
    my $result = $dbh->do($sql);

    logMessage(2, "Updated bound_no and ages for $result intervals");
}


# complete_bound_updates ( dbh, options )
# 
# Make sure that all changes to bound updates are propagated to referring bounds, and check for
# errors. Then clear the 'is_updated' flag on all updated bounds.

sub complete_bound_updates {
    
    my ($dbh, $options) = @_;
    
    my $dbstring = '';
    
    if ( $TABLE{TIMESCALE_BOUNDS} =~ /^([^.]+[.])/ )
    {
	$dbstring = $1;
    }
    
    logMessage(2, "Completing bound updates...");
    
    $dbh->do("CALL ${dbstring}complete_bound_updates");
    
    logMessage(2, "    complete_bound_updates");
    
    $dbh->do("CALL ${dbstring}check_updated_timescales");

    logMessage(2, "    check_updated_timescales");
    
    $dbh->do("CALL ${dbstring}unmark_updated_bounds");
    
    logMessage(2, "    unmark_updated_bounds");
    logMessage(2, "Done.");
}


# update_international_bounds ( dbh, options )
# 
# A few of the international boundaries are not correct in the source database. Correct them, and
# also set precision and 'spike' where appropriate.

sub update_international_bounds {
    
    my ($dbh, $options) = @_;
    
    logMessage(1, "Fixing international boundaries...");
    
    # First check that all of our fixes actually match up to one of the international time intervals.
    
    my $sql = "	SELECT fix.timescale_no, fix.interval_name
		FROM $TIMESCALE_FIX as fix left join $TABLE{TIMESCALE_INTS} as tsi using (interval_name)
			left join $TABLE{TIMESCALE_BOUNDS} as tsb on tsb.timescale_no = fix.timescale_no
			and tsb.interval_name = tsi.interval_name
		WHERE tsb.bound_no is null";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    if ( ref $result eq 'ARRAY' && @$result )
    {
	foreach my $r ( @$result )
	{
	    my $message = "  ERROR: fix for '$r->{interval_name}' in timescale $r->{timescale_no} has no match";
	    logMessage(2, $message);
	}
    }
    
    else
    {
	logMessage(2, "  All fixes are okay.");
    }
    
    # Then use this table to update the international timescale boundaries.
    
    $sql = "	UPDATE $TABLE{TIMESCALE_BOUNDS} as tsb join $TIMESCALE_FIX as fix using (timescale_no, interval_name)
		SET tsb.bound_type = coalesce(fix.bound_type, tsb.bound_type),
		    tsb.age = coalesce(fix.age, tsb.age),
		    tsb.age_prec = coalesce(fix.age_prec, tsb.age_prec),
		    tsb.age_error = coalesce(fix.age_error, tsb.age_error),
		    tsb.age_error_prec = coalesce(fix.age_error_prec, tsb.age_error_prec),
		    tsb.is_spike = coalesce(fix.bound_type, tsb.bound_type) = 'spike'";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    my $result = $dbh->do($sql);
    
    logMessage(2, "  Fixed $result international bounds from timescale_fix table") if $result && $result > 0;

    # Then propagate these values.

    $sql = "	UPDATE $TABLE{TIMESCALE_BOUNDS} as tsb join $TABLE{TIMESCALE_BOUNDS} as source
			on tsb.base_no = source.bound_no and tsb.bound_type = 'same'
		SET tsb.age = source.age,
		    tsb.age_prec = source.age_prec,
		    tsb.age_error = source.age_error,
		    tsb.age_error_prec = source.age_error_prec,
		    tsb.is_spike = source.is_spike
		WHERE source.timescale_no <= 10";
    
    print STDERR "$sql\n\n" if $options->{debug};
    
    my $result = $dbh->do($sql);
    
    logMessage(2, "  Propagated these changes to $result linked bounds") if $result && $result > 0;
}


# Load the international boundaries, if this hasn't already been done.

sub load_international_boundaries {
    
    my ($dbh) = @_;

    return if %INT_BOUND;
    
    my $sql = "	SELECT * FROM $TABLE{TIMESCALE_BOUNDS}
		WHERE timescale_no in (1,2,3,4,5) ORDER BY age, timescale_no";
    
    my $bounds = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    foreach my $r ( @$bounds )
    {
	$BOUND_BY_AGE{$r->{age}} ||= $r->{bound_no};
	$INT_BOUND{$r->{bound_no}} = $r;
    }
}


# Model one timescale by linking any of its bounds that match the international timescale, and
# expressing the others as fractions.

sub model_one_timescale {
    
    my ($dbh, $timescale_no, $options) = @_;
    
    $options ||= { };
    
    # Fetch the list of international boundaries, if that has not already been done.
    
    load_international_boundaries($dbh);
    
    # Start by querying all of the boundaries for the specified timescale.

    my $sql = " SELECT bound_no, age, interval_name FROM $TABLE{TIMESCALE_BOUNDS}
		WHERE timescale_no = $timescale_no ORDER BY age";

    my $bounds = $dbh->selectall_arrayref($sql, { Slice => { } });

    unless ( ref $bounds eq 'ARRAY' && @$bounds )
    {
	logMessage(2, "No boundaries found for timescale '$timescale_no'") unless $options->{silent};
	return;	
    }
    
    my @int_bound_ages = sort { $a <=> $b } keys %BOUND_BY_AGE;
    
    # Then go through the timescale bounds and see which if any match up to any of the
    # international bounds.
    
 BOUND:
    foreach my $r ( @$bounds )
    {
	my $bound_age = $r->{age};
	my $prev_age;
	my $prev_no;

	# Go through every international bound, looking for the best fit.

    AGE:
	foreach my $i ( 0..$#int_bound_ages )
	{
	    my $this_age = $int_bound_ages[$i];
	    my $this_no = $BOUND_BY_AGE{$this_age};
	    my $diff = abs($bound_age - $this_age);
	    
	    # If we found an exact match, then we have finished with this bound.
	    
	    if ( $diff == 0 )
	    {
		$r->{match_no} = $this_no;
		
		my $r1 = $INT_BOUND{$this_no};
		logMessage(2, "Exact match: $r->{age} ($r->{interval_name}) to $r1->{age} ($r1->{interval_name})")
		    unless $options->{silent};
		last AGE;
	    }

	    # Ditto if we found one that is a close match. But skip Olenekian unless the interval
	    # name matches as well. The current lower bound on the Olenekian is very close to an
	    # obsolete value for the lower bound on the Triassic.
	    
	    # elsif ( $bound_age > 0 && $diff < 0.1 && $diff < 0.05 * $this_age &&
	    # 	    ( $INT_BOUND{$this_no}{interval_name} ne 'Olenekian' ||
	    # 	      ( defined $r->{interval_name} && $r->{interval_name} eq 'Olenekian' ) ) )
	    # {
	    # 	$r->{match_no} = $this_no;
	    # 	$r->{is_modeled} = 1;
		
	    # 	my $r1 = $INT_BOUND{$this_no};
	    # 	logMessage(2, "Close match: $r->{age} ($r->{interval_name}) to $r1->{age} ($r1->{interval_name})")
	    # 	    unless $options->{silent};
	    # 	last AGE;
	    # }

	    # If we find ourselves in the middle of an interval, then compute how much of the way
	    # through the interval it lies.
	    
	    elsif ( $bound_age > $prev_age && $bound_age < $this_age )
	    {
		$r->{base_no} = $this_no;
		$r->{top_no} = $prev_no;

		my $fraction = ($this_age - $bound_age) / ($this_age - $prev_age);
		$r->{fraction} = sprintf("%.4f", $fraction);
		$r->{fraction_prec} = 4;
		$r->{is_modeled} = 1;
		
		my $rt = $INT_BOUND{$prev_no};
		my $rb = $INT_BOUND{$this_no};
		logMessage(2, "Fraction: $r->{age} ($r->{interval_name}) is $r->{fraction} between $rb->{age} ($rb->{interval_name}) and $rt->{age} ($rt->{interval_name})")
		    unless $options->{silent};
		last AGE;
	    }
	    
	    # Otherwise, go on to the next.
	    
	    else
	    {
		$prev_no = $this_no;
		$prev_age = $this_age;
	    }
	}

	# If we didn't find a match, something is wrong. Otherwise, update the bound.
	
	if ( $r->{match_no} || $r->{fraction} )
	{
	    update_modeled_bound($dbh, $r, $options);
	}

	else
	{
	    logMessage(2, "ERROR: no match for $r->{age} ($r->{interval_name})!!!");
	}
    }

    my $a = 1;	# we can stop here when debugging
}


sub update_modeled_bound {
    
    my ($dbh, $r, $options) = @_;
    
    my $sql;
    
    if ( $r->{match_no} )
    {
	my $values = "bound_type = 'same', base_no = $r->{match_no}";
	$values .= ", is_modeled = 1" if $r->{is_modeled};
	
	$sql = "UPDATE $TABLE{TIMESCALE_BOUNDS} SET $values
		WHERE bound_no = $r->{bound_no}";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$dbh->do($sql);
    }
    
    elsif ( $r->{fraction} )
    {
	$sql = "UPDATE $TABLE{TIMESCALE_BOUNDS} SET bound_type = 'fraction', 
			base_no = $r->{base_no}, range_no = $r->{top_no}, is_modeled = 1,
			fraction = $r->{fraction}, fraction_prec = $r->{fraction_prec}
		WHERE bound_no = $r->{bound_no}";
	
	print STDERR "$sql\n\n" if $options->{debug};
	
	$dbh->do($sql);
    }
}



# Given a timescale number, check to see which of the boundaries might correspond to international
# interval boundaries, and model the rest as fractions.

sub model_timescale {
    
    my ($dbh, $timescale_no, $options) = @_;
    
    # Check that the argument is in the proper range.
    
    unless ( $timescale_no && $timescale_no =~ /^\d+$/ )
    {
	print STDERR "Invalid timescale number '$timescale_no'. Aborting.\n";
	return;
    }
    
    # Then query all of the boundaries for the specified timescale.
    
    my $sql = " SELECT tsb.*, tsi.interval_name FROM $TABLE{TIMESCALE_BOUNDS} as tsb 
		WHERE timescale_no = $timescale_no ORDER BY age";
    
    my $ts_res = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    unless ( ref $ts_res eq 'ARRAY' && @$ts_res )
    {
	logMessage(2, "No boundaries found for timescale '$timescale_no'");
	return;
    }
    
    # Now query all of the international boundaries.
    
    my (@INTERNATIONAL, %INT, %BOUND_BY_AGE, @AGES);
    
    $sql = "	SELECT tsb.*, tsi.interval_name FROM $TABLE{TIMESCALE_BOUNDS} as tsb 
			left join $TABLE{TIMESCALE_INTS} as tsi using (interval_no)
		WHERE timescale_no in (1,2,3,4,5) ORDER BY age, timescale_no desc";
    
    my $int_res = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    foreach my $r ( @$int_res )
    {
	push @INTERNATIONAL, $r;
	push @{$BOUND_BY_AGE{$r->{age}}}, $r;
	$INT{$r->{bound_no}} = $r;
    }

    @AGES = sort { $a <=> $b } keys %BOUND_BY_AGE;
    
    # Then go through the interval bounds and see which if any match up to international bounds.

 BOUND:
    foreach my $r ( @$ts_res )
    {
	foreach my $a1 ( @AGES )
	{
	    my $diff = abs($r->{age} - $a1);
	    my $r1 = match_age($a1, \%BOUND_BY_AGE);
	    
	    if ( $r->{age} == $a1 )
	    {
		logMessage(2, "Matched $r->{age} ($r->{interval_name}) to $r1->{age} ($r1->{interval_name})");
		$r->{match} = $r1->{bound_no};
		next BOUND;
		
	    }
	    
	    elsif ( $r->{age} > 0 && $diff < 2.0 && $diff < 0.05 * $r1->{age} )
	    {
		my $answer = logQuestion("Match $r->{age} ($r->{interval_name}) to $r1->{age} ($r1->{interval_name})?");
		if ( $answer && $answer =~ /y/i )
		{
		    $r->{match} = $r1->{bound_no};
		    next BOUND;
		}
	    }
	}
    }

    # Now go back through them again. For each one that doesn't match, compute a fractional offset based on
    # the ORIGINAL ages (not the matched ones).
    
    my ($base_age, $base_no);
    my ($top_age, $top_no);
    
    foreach my $r ( reverse @$ts_res )
    {
	if ( $r->{match} )
	{
	    $base_age = $r->{age};
	    $base_no = $r->{match};
	}
	
	elsif ( $base_age )
	{
	    $r->{base_age} = $base_age;
	    $r->{base_no} = $base_no;
	}
    }
    
    foreach my $r ( @$ts_res )
    {
	if ( $r->{match} )
	{
	    $top_age = $r->{age};
	    $top_no = $r->{match};
	}
	
	elsif ( defined $top_age )
	{
	    $r->{top_age} = $top_age;
	    $r->{top_no} = $top_no;
	}

	if ( $r->{base_no} && $r->{top_no} )
	{
	    my $fraction = ($r->{base_age} - $r->{age}) / ($r->{base_age} - $r->{top_age});
	    $r->{fraction} = sprintf("%.1f", $fraction);
	}
    }
    
    # Now print them out
    
    logMessage(2, "");
    logMessage(2, "Modeled boundaries:");
    
    foreach my $r ( @$ts_res )
    {
	if ( $r->{match} )
	{
	    my $name = $INT{$r->{match}}{interval_name};
	    logMessage(2, "$r->{age} ($r->{interval_name}) : matches $name [$r->{match}]");
	}

	elsif ( defined $r->{fraction} )
	{
	    logMessage(2, "$r->{age} ($r->{interval_name}) : $r->{fraction}\% : $r->{base_age} ($r->{base_no}) - $r->{top_age} ($r->{top_no})");
	}

	else
	{
	    logMessage(2, "$r->{age} ($r->{interval_name}) : absolute");
	}
    }

    # Ask if the user wants to update, and if so then do it.

    my $answer = logQuestion("Update these boundaries?");

    if ( $answer && $answer =~ /y/i )
    {
	$dbh->do("START TRANSACTION");

	try {
	    update_modeled($dbh, $ts_res, $options);
	}

        catch {
	    $dbh->do("ROLLBACK");
	    logMessage(2, "Aborted...");
	};

	$dbh->do("COMMIT");	
    }
}


sub match_age {
    
    my ($age, $intervals) = @_;

    my $limit;
    
    if ( $age <= 66 ) { $limit = 2; }
    elsif ( $age <= 542 ) { $limit = 3; }
    else { $limit = 4; }

    foreach my $r ( @{$intervals->{$age}} )
    {
	return $r if $r->{timescale_no} <= $limit;
    }

    return $intervals->{$age}[0];
}


sub update_modeled {
    
    my ($dbh, $ints, $options) = @_;

    my $sql;
    
    foreach my $r ( @$ints )
    {
	if ( $r->{match} )
	{
	    $sql = "
		UPDATE $TABLE{TIMESCALE_BOUNDS} SET bound_type = 'same', base_no = $r->{match}
		WHERE bound_no = $r->{bound_no}";

	    print STDERR "$sql\n\n" if $options->{debug};

	    $dbh->do($sql);
	}

	elsif ( $r->{fraction} )
	{
	    $sql = "
		UPDATE $TABLE{TIMESCALE_BOUNDS} SET bound_type = 'fraction', base_no = $r->{base_no},
			range_no = $r->{top_no}, offset = $r->{fraction}, is_modeled = 1
		WHERE bound_no = $r->{bound_no}";

	    print STDERR "$sql\n\n" if $options->{debug};

	    $dbh->do($sql);
	}
    }
}


# establish_procedures ( dbh )
#
# Create or replace the triggers and stored procedures necessary for the timescale tables to work
# properly.

sub establish_procedures {
    
    my ($dbh, $options) = @_;

    # If these procedures are being established in a database other than the main one, adjust the procedure
    # names accordingly. We test this by checking whether the name of the bounds table is qualified
    # with a database name. If that is the case, then all triggers will be created in the same database.
    
    my $dbstring = '';
    
    if ( $TABLE{TIMESCALE_BOUNDS} =~ /^([^.]+[.])/ )
    {
	$dbstring = $1;
    }
    
    # Now create or replace the necessary triggers.
    
    logMessage(1, "Creating or replacing stored procedures...");
    
    logMessage(2, "    ${dbstring}complete_bound_updates");
    
    $dbh->do("DROP PROCEDURE IF EXISTS ${dbstring}complete_bound_updates");
    
    $dbh->do("CREATE PROCEDURE ${dbstring}complete_bound_updates ( )
	BEGIN

	SET \@row_count = 1;
	SET \@age_iterations = 0;
	
	# For all updated records where the precision fields have not been
	# set, set them to the position of the last nonzero digit after the
	# decimal place.
	
	UPDATE $TABLE{TIMESCALE_BOUNDS}
	SET age_prec = coalesce(age_prec, length(regexp_substr(age, '(?<=[.])\\\\d*?(?=0*\$)'))),
	    age_error_prec = coalesce(age_error_prec, 
				length(regexp_substr(age_error, '(?<=[.])\\\\d*?(?=0*\$)'))),
	    fraction_prec = coalesce(fraction_prec, length(regexp_substr(fraction, '(?<=[.])\\\\d*?(?=0*\$)')))
	WHERE is_updated and (age_prec is null or age_error_prec is null or fraction_prec is null);
	
	# Now update the ages and flags on all bounds that are marked as is_updated.  If
	# this results in any updated records, repeat the process until no
	# records change. We put a limit of 20 on the number of iterations.
	
	WHILE \@row_count > 0 AND \@age_iterations < 20 DO
	    
	    UPDATE $TABLE{TIMESCALE_BOUNDS} as tsb
		left join $TABLE{TIMESCALE_BOUNDS} as `base` on base.bound_no = tsb.base_no
		left join $TABLE{TIMESCALE_BOUNDS} as `range` on range.bound_no = tsb.range_no
		left join $TABLE{TIMESCALE_BOUNDS} as `csource` on csource.bound_no = tsb.color_no
	    SET tsb.is_updated = 1,
		tsb.is_spike = case tsb.bound_type
			when 'same' then base.is_spike
			else tsb.is_spike
			end,
		tsb.age = case tsb.bound_type
			when 'same' then base.age
			when 'fraction' then if(tsb.age_updated, tsb.age,
						round(base.age - tsb.fraction * ( base.age - range.age ),
						      tsb.age_prec))
					           # greatest(least(coalesce(base.age_prec, 0), 
						   #                coalesce(range.age_prec, 0)),
						   # 	    if(base.age > 600, 0, 1)))
			else tsb.age
			end,
		tsb.age_prec = case tsb.bound_type
			when 'same' then base.age_prec
			# when 'fraction' then least(coalesce(base.age_prec, 0),
			# 			   coalesce(range.age_prec, 0))
			else tsb.age_prec
			end,
		tsb.age_error = case tsb.bound_type
			when 'same' then base.age_error
			# when 'fraction' then coalesce(greatest(base.age_error, range.age_error),
			# 			     base.age_error, range.age_error)
			else tsb.age_error
			end,
		tsb.age_error_prec = case tsb.bound_type
			when 'same' then base.age_error_prec
			# when 'fraction' then least(coalesce(base.age_error_prec, 0),
			# 			  coalesce(range.age_error_prec, 0))
			else tsb.age_error_prec
			end,
		tsb.is_spike = case tsb.bound_type
			when 'spike' then 1
			when 'same' then base.is_spike
			else 0
			end,
		tsb.color = coalesce(csource.color, tsb.color)
	    WHERE base.is_updated or range.is_updated or tsb.is_updated;
	    
	    SET \@row_count = ROW_COUNT();
	    SET \@age_iterations = \@age_iterations + 1;
	    
	    # SET \@debug = concat(\@debug, \@cnt, ' : ');
	    
	    # Now make sure that every fractional bound that explicitly had its age set has the
	    # proper fraction set. We do this step last so that if the base and/or range bound are
	    # also being updated then they will attain their final values before we do this
	    # computation. As part of this step, we clear the 'age_updated' flag.
	    
	    UPDATE $TABLE{TIMESCALE_BOUNDS} as tsb
		left join $TABLE{TIMESCALE_BOUNDS} as `base` on base.bound_no = tsb.base_no
		left join $TABLE{TIMESCALE_BOUNDS} as `range` on range.bound_no = tsb.range_no
	    SET tsb.fraction = if(base.age is not null and range.age is not null and tsb.age is not null and
					base.age <> range.age,
				  round((base.age - tsb.age) / (base.age - range.age), 4),
				  null),
		tsb.age_updated = 0
	    WHERE tsb.age_updated;
	    
	END WHILE;
	
	END;");
    
    logMessage(2, "    ${dbstring}update_timescale_ages");
    
    $dbh->do("DROP PROCEDURE IF EXISTS ${dbstring}update_timescale_ages");
    
    $dbh->do("CREATE PROCEDURE ${dbstring}update_timescale_ages ( t int unsigned )
	BEGIN
	
	UPDATE $TABLE{TIMESCALE_DATA} as `ts` join
	    (SELECT age, age_prec, timescale_no FROM $TABLE{TIMESCALE_BOUNDS}
	     WHERE timescale_no = t
	     ORDER BY age LIMIT 1) as `min` using (timescale_no)
	SET ts.min_age = min.age
	WHERE timescale_no = t;
	
	UPDATE $TABLE{TIMESCALE_DATA} as `ts` join
	    (SELECT age, age_prec, timescale_no FROM $TABLE{TIMESCALE_BOUNDS}
	     WHERE timescale_no = t
	     ORDER BY age desc LIMIT 1) as `max` using (timescale_no)
	SET ts.max_age = max.age
	WHERE timescale_no = t;
	
	END;");
    
    logMessage(2, "    ${dbstring}check_updated_timescales");
    
    $dbh->do("DROP PROCEDURE IF EXISTS ${dbstring}check_updated_bounds");
    $dbh->do("DROP PROCEDURE IF EXISTS ${dbstring}check_updated_timescales");
    
    $dbh->do("CREATE PROCEDURE ${dbstring}check_updated_timescales ( )
	BEGIN
	
	UPDATE $TABLE{TIMESCALE_BOUNDS} as `tsb` join $TABLE{TIMESCALE_DATA} as `ts` using (timescale_no)
	SET ts.is_updated = 1 WHERE tsb.is_updated;
	
	UPDATE $TABLE{TIMESCALE_BOUNDS} as `tsb` join $TABLE{TIMESCALE_DATA} as `ts` using (timescale_no)
		left join $TABLE{TIMESCALE_BOUNDS} as `upper` on upper.bound_no = tsb.top_no
		left join $TABLE{TIMESCALE_BOUNDS} as `base` on base.bound_no = tsb.base_no
		left join $TABLE{TIMESCALE_BOUNDS} as `range` on range.bound_no = tsb.range_no
	SET tsb.has_error =
		if((tsb.age is null) or
		   (upper.age is not null and tsb.age <= upper.age) or
		   (upper.timescale_no <> tsb.timescale_no) or
		   (tsb.bound_type = 'fraction' and (range.bound_no is null or
						     tsb.fraction is null or 
						     not tsb.fraction between 0 and 1)) or
		   (tsb.bound_type in ('base', 'fraction') and base.bound_no is null),
		   1, 0)
	WHERE ts.is_updated;
	
	UPDATE $TABLE{TIMESCALE_BOUNDS} as `tsb` join
	    (SELECT timescale_no, b.interval_name, count(*) as `c`
	     FROM $TABLE{TIMESCALE_BOUNDS} as `b` join $TABLE{TIMESCALE_DATA} as `ts` using (timescale_no)
	     WHERE b.interval_name <> '' and ts.is_updated
	     GROUP BY timescale_no, interval_name HAVING `c`>1) as `duplicate_bounds` using (timescale_no, interval_name) 
	SET tsb.has_error = 1;
	
	UPDATE $TABLE{TIMESCALE_DATA} as `ts` join
	    (SELECT timescale_no, min(b.age) as `min`, max(b.age) as `max`, max(b.has_error) as `has_error`
	     FROM $TABLE{TIMESCALE_BOUNDS} as `b` join $TABLE{TIMESCALE_DATA} as `t` using (timescale_no)
	     WHERE t.is_updated GROUP BY timescale_no) as `all_bounds` using (timescale_no)
	SET ts.min_age = all_bounds.min,
	    ts.max_age = all_bounds.max,
	    ts.has_error = all_bounds.has_error,
	    ts.bounds_updated = now();
	
	END;");
    
    logMessage(2, "    ${dbstring}link_timescale_bounds");
    
    $dbh->do("DROP PROCEDURE IF EXISTS ${dbstring}link_timescale_bounds");
    
    $dbh->do("CREATE PROCEDURE ${dbstring}link_timescale_bounds ( t int unsigned )
    	BEGIN
	
    	SET \@last_bound := 0, \@save_bound := 0;
	
    	UPDATE $TABLE{TIMESCALE_BOUNDS}
    	SET top_no = last_value(\@save_bound := \@last_bound, \@last_bound := bound_no, \@save_bound)
    	WHERE timescale_no = t ORDER BY age;
	
    	END;");
    
    logMessage(2, "    ${dbstring}unmark_updated (dropped)");
    
    $dbh->do("DROP PROCEDURE IF EXISTS ${dbstring}unmark_updated");
    
    # $dbh->do("CREATE PROCEDURE unmark_updated ( )
    # 	BEGIN
	
    # 	UPDATE $TABLE{TIMESCALE_BOUNDS} SET is_updated = 0;
    # 	# UPDATE $TABLE{TIMESCALE_DATA} SET is_updated = 0;
	
    # 	END;");
    
    logMessage(2, "    ${dbstring}unmark_updated_bounds");
    
    $dbh->do("DROP PROCEDURE IF EXISTS ${dbstring}unmark_updated_bounds");
    
    $dbh->do("CREATE PROCEDURE ${dbstring}unmark_updated_bounds ( )
    	BEGIN
	
    	UPDATE $TABLE{TIMESCALE_BOUNDS} SET is_updated = 0;
	UPDATE $TABLE{TIMESCALE_DATA} SET is_updated = 0;
	
    	END;");
    
    logMessage(1, "Done.");
}


# establish_triggers ( dbh )
#
# Create or replace the triggers and stored procedures necessary for the timescale tables to work
# properly.

sub establish_triggers {
    
    my ($dbh, $options) = @_;

    # If these triggers are being established in a database other than the main one, adjust the trigger
    # name accordingly. We test this by checking whether the name of the bounds table is qualified
    # with a database name. If that is the case, then all triggers will be creatd in the same database.
    
    my $dbstring = '';
    
    if ( $TABLE{TIMESCALE_BOUNDS} =~ /^([^.]+[.])/ )
    {
	$dbstring = $1;
    }
    
    # Now create or replace the necessary triggers.
    
    logMessage(1, "Creating or replacing triggers...");
    
    logMessage(2, "    ${dbstring}insert_bound on $TABLE{TIMESCALE_BOUNDS}");
    
    $dbh->do("DROP TRIGGER IF EXISTS ${dbstring}insert_bound");
    
    $dbh->do("CREATE TRIGGER ${dbstring}insert_bound
	BEFORE INSERT ON $TABLE{TIMESCALE_BOUNDS} FOR EACH ROW
	BEGIN
	    DECLARE ts_interval_type varchar(10);
	    
	    IF NEW.timescale_no > 0 THEN
		SELECT timescale_type INTO ts_interval_type
		FROM $TABLE{TIMESCALE_DATA} WHERE timescale_no = NEW.timescale_no;
		
		IF NEW.interval_type is null or NEW.interval_type = ''
		THEN SET NEW.interval_type = ts_interval_type; END IF;
	    END IF;
	    
	    SET NEW.is_updated = 1;
	END;");
    
    logMessage(2, "    ${dbstring}update_bound on $TABLE{TIMESCALE_BOUNDS}");
    
    $dbh->do("DROP TRIGGER IF EXISTS ${dbstring}update_bound");
    
    $dbh->do("CREATE TRIGGER ${dbstring}update_bound
	BEFORE UPDATE ON $TABLE{TIMESCALE_BOUNDS} FOR EACH ROW
	BEGIN
	    IF  OLD.bound_type <> NEW.bound_type or
	        OLD.interval_name <> NEW.interval_name or
	        OLD.top_no <> NEW.top_no or OLD.base_no <> NEW.base_no or
		OLD.range_no <> NEW.range_no or OLD.color_no <> NEW.color_no or
		OLD.age <> NEW.age or OLD.age_prec <> NEW.age_prec or
		OLD.age_error <> NEW.age_error or OLD.age_error_prec <> NEW.age_error_prec or
		OLD.fraction <> NEW.fraction or OLD.fraction_prec <> NEW.fraction_prec or 
		OLD.is_spike <> NEW.is_spike or OLD.color <> NEW.color
	    THEN
	        SET NEW.is_updated = 1; END IF;
	END;");
    
    logMessage(1, "Done.");
}

1;
