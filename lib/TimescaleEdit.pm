# 
# The Paleobiology Database
# 
#   TimescaleEdit.pm
# 

package TimescaleEdit;

use strict;

use Carp qw(carp croak);
use Try::Tiny;

use TableDefs qw(%TABLE get_table_property);
use TimescaleDefs;
use ExternalIdent qw(%IDRE %IDP);

use base 'EditTransaction';

our (@CARP_NOT) = qw(EditTransaction);

our ($UPDATE_LIMIT) = 10;	# Limit on the number of iterations when propagating changes to
                                # updated records.

our ($TIMESCALE_KEY) = 'timescale_no';

{
    TimescaleEdit->register_conditions(
	C_BREAK_DEPENDENCIES => "There are intervals in other timescales dependent on this one. Allow BREAK_DEPENDENCIES to break these dependencies.",
	E_TIMESCALE_HAS_ERRORS => "The timescale '%1' has inconsistencies in its bounds.");

    TimescaleEdit->register_allowances('BREAK_DEPENDENCIES');
}


# The following methods override methods from EditTransaction.pm:
# ---------------------------------------------------------------

# We need to include a precision along with every numeric attribute that is being set.

sub validate_action {

    my ($edt, $action, $operation, $table) = @_;

    # If the action does not include a record, we have nothing to do.
    
    my $record = $action->record || return;
    
    # If the table is 'TIMESCALE_BOUNDS', then check the bound attributes for consistency.
    #
    # If the bound type is 'absolute' or 'spike', then we must have an age. If the bound type is
    # 'same' or 'fraction', then we must have a 'base_no' and possibly a 'range_no'.
    #
    # If the interval name is not empty, then we must have a 'top_no'.

    if ( $table eq 'TIMESCALE_BOUNDS' && $operation ne 'delete' )
    {
	my $v = { };
	
	# For an 'update' operation, fetch the old values first. Any column not mentioned in the action
	# record will of course retain its old value.
	
	if ( $operation eq 'update' )
	{
	    $v = $edt->fetch_old_record($action);
	}
	
	$v->{bound_type} = $record->{bound_type} if exists $record->{bound_type};
	$v->{interval_name} = $record->{interval_name} if exists $record->{interval_name};
	$v->{age} = $record->{age} if exists $record->{age};
	$v->{base_no} = $record->{base_id} || $record->{base_no} if exists $record->{base_id} || exists $record->{base_no};
	$v->{range_no} = $record->{range_id} || $record->{range_no} if exists $record->{range_id} || exists $record->{range_no};
	$v->{top_no} = $record->{top_id} || $record->{top_no} if exists $record->{top_id} || $record->{top_no};
	
	# Now check the required fields that depend on the value of bound_type. In the case of
	# type 'fraction', if the age is being set then we compute the fraction that will generate
	# that and ignore any fraction that was provided.
	
	unless ( $v->{bound_type} )
	{
	    $edt->add_condition('E_REQUIRED', 'bound_type');
	    $action->ignore_column('bound_type');
	}
	
	elsif ( $v->{bound_type} eq 'same' )
	{
	    $edt->add_condition('E_REQUIRED', 'base_id') unless $v->{base_no};
	}
	
	elsif ( $v->{bound_type} eq 'fraction' )
	{
	    $edt->add_condition('E_REQUIRED', 'base_id') unless $v->{base_no};
	    $edt->add_condition('E_REQUIRED', 'range_id') unless $v->{range_no};
	    
	    if ( defined $record->{age} && $record->{age} =~ /\d/ )
	    {
		$record->{age_updated} = 1;
	    }
	}
	
	else
	{
	    $edt->add_condition('E_REQUIRED', 'age') unless defined $v->{age} && $v->{age} ne '';
	}
	
	# Then check for 'top_id' if the interval name is not empty.
	
	if ( defined $v->{interval_name} && $v->{interval_name} ne '' )
	{
	    $edt->add_condition('E_REQUIRED', 'top_id') unless $v->{top_no};
	}
	
	# Look for any of the following fields in the action record. If they are specified, then set
	# the corresponding _prec field.
	
	foreach my $f ( 'age', 'age_error', 'fraction' )
	{
	    if ( defined $record->{$f} && $record->{$f} ne '' )
	    {
		if ( $record->{$f} =~ qr{ [.] (\d*) $ }xs )
		{
		    $record->{"${f}_prec"} = length($1);
		}
		
		else
		{
		    $record->{"${f}_prec"} = 0;
		}
	    }
	}
    }

    # If the table is 'TIMESCALE_DATA', then check the timescale attributes for consistency. The
    # priority can only be increased above 9 by administrators.
    
    elsif ( $table eq 'TIMESCALE_DATA' and $operation ne 'delete' )
    {
	my $perm = $action->permission;
	
	if ( defined $record->{priority} && $record->{priority} > 9 &&
	     $perm ne 'admin' )
	{
	    $edt->add_condition('E_PERM_COL', 'priority');
	}
    }
}


# Before we execute certain actions, we must check for conditions specific to this data type and
# execute some auxiliary actions.

sub before_action {
    
    my ($edt, $action, $operation, $table) = @_;

    # Keep track of which timescales were touched in any way. We will use this at finalization.
    
    if ( my $record = $action->record )
    {
	if ( ref $record eq 'HASH' )
	{
	    if ( my $timescale_no = $record->{timescale_no} || $record->{timescale_id} )
	    {
		$edt->set_attr_key('updated_timescale', $timescale_no, 1);
	    }
	    
	    elsif ( my $bound_no = $record->{bound_no} || $record->{bound_id} )
	    {
		$edt->set_attr_key('updated_bound', $bound_no, 1);
	    }	    
	}
    }

    # Now carry out necessary checks before deletion.
    
    if ( $operation eq 'delete' && $table eq 'TIMESCALE_DATA' )
    {
	return $edt->before_delete_timescale($action);
    }
    
    elsif ( $operation eq 'delete' && $table eq 'TIMESCALE_BOUNDS' )
    {
	return $edt->before_delete_bounds($action);
    }
    
    elsif ( $operation eq 'delete_cleanup' && $table eq 'TIMESCALE_BOUNDS' )
    {
	return $edt->before_delete_bounds($action);
    }
    
    # elsif ( $operation eq 'update' && $table eq 'TIMESCALE_DATA' )
    # {
    # 	return $edt->before_update_timescale($action, $operation);
    # }
    
    # elsif ( $operation eq 'replace' && $table eq 'TIMESCALE_DATA' )
    # {
    # 	return $edt->before_update_timescale($action, $operation);
    # }
}


# finalize_transaction ( table )
#
# We have a lot to do at the end of each transaction. In particular, we will need to check bounds
# on all updated timescales and also recompute all intervals whose authority timescale has been
# deleted or had its status change.

sub finalize_transaction {

    my ($edt, $table) = @_;
    
    my $dbh = $edt->dbh;
    my $result;
    my $sql;

    # Complete and propagate all bound updates, and check all bounds in any timescale that has at
    # least one updated bound.
    
    my $dbstring = '';
    
    if ( $TABLE{TIMESCALE_BOUNDS} =~ /^([^.]+[.])/ )
    {
	$dbstring = $1;
    }
    
    $result = $dbh->do("CALL ${dbstring}complete_bound_updates");
    $result = $dbh->do("CALL ${dbstring}check_updated_timescales");
    $result = $dbh->do("CALL ${dbstring}unmark_updated_bounds");
    
    return;
    
    # # Start by keeping track of which timescales were updated.
    
    # my @timescale_list = $edt->get_attr_keys('updated_timescale');
    # my @bound_list = $edt->get_attr_keys('updated_bound');
    # my $active_str;

    # if ( @bound_list )
    # {
    # 	my $bound_str = join(',', @bound_list);
	
    # 	$sql = "SELECT group_concat(distinct timescale_no) FROM $TABLE{TIMESCALE_BOUNDS}
    # 		WHERE bound_no in ($bound_str)";
	
    # 	$edt->debug_line("$sql\n\n");
	
    # 	my ($other_ts) = $dbh->selectrow_array($sql);
	
    # 	push @timescale_list, $other_ts if ref $other_ts eq 'ARRAY';
    # }
    
    # if ( @timescale_list )
    # {
    # 	my $timescale_str = join(',', @timescale_list);
	
    # 	$sql = "SELECT group_concat(distinct timescale_no) FROM $TABLE{TIMESCALE_DATA}
    # 		WHERE timescale_no in ($timescale_str) and is_visible";
	
    # 	$edt->debug_line("$sql\n\n");
	
    # 	($active_str) = $dbh->selectcol_arrayref($sql);
    # }
    
    # return unless $active_str;
    # return;
    # # If we get here, then we have updated at least one active timescale. So check to see if any
    # # of its interval names do not correspond to an existing record. If so, they will need to be
    # # created.

    # $sql = "SELECT distinct interval_name FROM $TABLE{TIMESCALE_BOUNDS} as tsb
    # 		left join $TABLE{TIMESCALE_INTS} as tsi using (interval_name)
    # 		WHERE timescale_no in ($active_str) and tsi.interval_name is null";
    
    # $edt->debug_line("$sql\n\n");

    # my $missing_intervals = $dbh->selectcol_arrayref($sql);

    # if ( ref $missing_intervals eq 'ARRAY' )
    # {
    # 	foreach my $name ( @$missing_intervals )
    # 	{
    # 	    if ( $edt->allows('CREATE_INTERVALS') )
    # 	    {
    # 		my $quoted = $dbh->quote($name);
		
    # 		$sql = "INSERT INTO $TABLE{TIMESCALE_INTS} (interval_name) VALUES ($quoted)";

    # 		$edt->debug_line("$sql\n\n");

    # 		$dbh->do($sql);
    # 	    }

    # 	    else
    # 	    {
    # 		$edt->add_condition('main', 'C_CREATE_INTERVALS', $name);
    # 	    }
    # 	}
    # }

    # # Now make sure that all of the interval records corresponding to intervals in any of the
    # # active timescales are updated to match the values in the most authoritative timescale.

    # $sql = "UPDATE $TABLE{TIMESCALE_INTS} as tsi join (
    # 	WITH a2 as (SELECT interval_name, authority_level, timescale_no, 
    # 		max((authority_level+1)*10000) - max((authority_level+1)*10000 - timescale_no) as ts 
    # 		FROM timescale_bounds join timescales using (timescale_no)
    # 		WHERE interval_name <> '' and interval_name in 
    # 			(SELECT distinct interval_name FROM timescale_bounds where timescale_no in ($active_str))
    # 		GROUP by interval_name)
    # 	SELECT lower.interval_name, lower.age as early_age, lower.age_prec as early_age_prec,
    # 		upper.age as late_age, upper.age_prec as late_age_prec, lower.timescale_no, lower.color
    # 	FROM a2 join timescale_bounds as lower using (interval_name, timescale_no)
    # 		join timescale_bounds as upper on upper.bound_no = lower.top_no) as a using (interval_name)
    # 	SET tsi.early_age = a.early_age,
    # 	    tsi.early_age_prec = a.early_age_prec,
    # 	    tsi.late_age = a.late_age,
    # 	    tsi.late_age_prec = a.late_age_prec,
    # 	    tsi.color = a.color,
    # 	    tsi.authority_timescale_no = a.timescale_no";
    
    # $edt->debug_line("$sql\n\n");

    # $result = $dbh->do($sql);
    
    # update timescale_ints as tsi join (with a2 as (select interval_name, authority_level, timescale_no, max((authority_level+1)*10000) - max((authority_level+1)*10000 - timescale_no) as ts from timescale_bounds join timescales using (timescale_no) where interval_name <> '' and interval_name in (select distinct interval_name from timescale_bounds where timescale_no = 1) group by interval_name) select lower.interval_name, lower.age as early_age, lower.age_prec as early_age_prec, upper.age as late_age, upper.age_prec as late_age_prec, lower.timescale_no, lower.color from a2 join timescale_bounds as lower using (interval_name, timescale_no) join timescale_bounds as upper on upper.bound_no = lower.top_no) as a using (interval_name) set tsi.early_age = a.early_age, tsi.early_age_prec = a.early_age_prec, tsi.late_age = a.late_age, tsi.late_age_prec = a.late_age_prec, tsi.color = a.color, tsi.authority_timescale_no = a.timescale_no;	
    
    
    
    # my @timescales = $edt->get_attr_keys('update_bound_list');
    
    # foreach my $t ( @timescales )
    # {
    # 	$result = $dbh->do("CALL check_bound_list($t)");
    # 	# $result = $dbh->do("CALL update_bound_list($t)");
    # }
    
    # my @timescales = $edt->get_attr_keys('update_authority');
    
    # foreach my $t ( @timescales )
    # {
    # 	$edt->update_authority($t);
    # }
    
    # while ( @timescales )
    # {
    # 	my $interval_nos = join(',', splice(@intervals,0,100));
	
    # 	$result = $dbh->do("CALL update_interval_definitions($interval_nos)");
    # }
}


# before_update_timescale ( action )
#
# If we change the authority level on a timescale, or change its is_active status, we must
# recompute the definition of every interval that is referenced by that timescale. If we are
# replacing the entire record, we recompute them regardless.

sub before_update_timescale {
    
    my ($edt, $action, $operation) = @_;
    
    # return if $action eq 'update' && ! ( $action->has_field('authority_level') ||
    # 					 $action->has_field('is_active') );
    
    # my (@ids) = $action->keylist;
    
    # foreach my $id ( @ids )
    # {
    # 	$edt->set_attr_key('update_authority', $id, 1);
    # }
    
    # my $dbh = $edt->dbh;
    # my $keyexpr = $action->keyexpr;
    
    # my $auth = $dbh->selectcol_arrayref("SELECT interval_name
    # 		FROM $TABLE{TIMESCALE_BOUNDS} WHERE timescale_no in ($keyexpr)");
    
    # if ( ref $auth eq 'ARRAY' && @$auth )
    # {
    # 	foreach my $i ( @$auth )
    # 	{
    # 	    $edt->set_attr_key('update_intervals', $i, 1);
    # 	}
    # }
}


# before_delete_timescale ( action )
# 
# If we are deleting a timescale, we need to check that no bounds from any other timescale depend
# on this one. If so, then we return the caution C_BREAK_DEPENDENCIES unless BREAK_DEPENDENCIES is
# allowed. If it is, we break any dependencies. If there are any intervals for which this
# timescale is the authority, they must be recalculated. Finally, we must delete all boundaries in
# the specified timescale before the timescale itself is deleted. All of this takes place inside
# the same transaction, so if any errors occur the transaction will be aborted.

sub before_delete_timescale {

    my ($edt, $action) = @_;

    my $keystring = $action->keystring;
    
    # If we have no key expression, something is very wrong.
    
    unless ( $keystring )
    {
	$edt->add_condition($action, 'E_EXECUTE', "E0001");
	return;
    }
    
    # Otherwise, we need to check whether there are any bounds dependent on the ones being
    # deleted.
    
    return if $edt->check_dependencies($action, "base.timescale_no in ($keystring)");

    # If we there are no dependencies, or if they have been broken, then delete all bounds
    # corresponding to the timescales to be deleted.

    my $dbh = $edt->dbh;
    
    my $sql = "DELETE FROM $TABLE{TIMESCALE_BOUNDS} WHERE timescale_no in ($keystring)\n";
    
    $edt->debug_line($sql);
    
    my $result = $dbh->do($sql);

    $edt->debug_line("Deleted $result bounds.\n") if $result && $result > 0;
}


sub before_delete_bounds {

    my ($edt, $action) = @_;
    
    my $keyexpr = $action->keyexpr;
    
    # If we have no key expression, something is very wrong.
    
    unless ( $keyexpr )
    {
	$edt->add_condition($action, 'E_EXECUTE', "E0002");
	return;
    }
    
    # Otherwise, we need to check whether there are any bounds dependent on the ones being
    # deleted.
    
    $keyexpr =~ s/(\w+_no)/base.$1/g;
    
    $edt->check_dependencies($action, $keyexpr);
}


# check_dependencies ( action, selector )
#
# This method is called before every action that involves the deletion of bound records. It checks
# whether any bounds from other timescales depend on the bounds to be deleted. If so, and if the
# transaction allows BREAK_DEPENDENCIES, then all references are set to 0. If this allowance is
# not present, a BREAK_DEPENDENCIES caution is returned and this routine returns true. In all
# other cases, it returns false.

sub check_dependencies {

    my ($edt, $action, $selector) = @_;

    my $dbh = $edt->dbh;
    my $result;
    
    # The first thing we need to do is check if there are any dependencies on the bounds to be
    # deleted.
    
    my $link = "$TABLE{TIMESCALE_BOUNDS} as base join $TABLE{TIMESCALE_BOUNDS} as dep
		on base.timescale_no <> dep.timescale_no and base.bound_no =";

    my $sql = "SELECT count(*) FROM (
	SELECT dep.bound_no FROM $link dep.top_no WHERE $selector UNION ALL
	SELECT dep.bound_no FROM $link dep.base_no WHERE $selector UNION ALL
	SELECT dep.bound_no FROM $link dep.range_no WHERE $selector UNION ALL
	SELECT dep.bound_no FROM $link dep.color_no WHERE $selector) as deps\n";
    
    $edt->debug_line($sql);
    
    my ($has_deps) = $dbh->selectrow_array($sql);
    
    # If there are no dependencies, then return false.
    
    if ( ! $has_deps )
    {
	return;
    }
    
    # If there is at least one, then the outcome depends on whether the allowance
    # BREAK_DEPENDENCIES is active. If it is, then break the dependencies and return false.
    
    elsif ( $edt->allows('BREAK_DEPENDENCIES') )
    {
	$result = $dbh->do("UPDATE $link dep.top_no SET dep.top_no = 0 WHERE $selector");
	$result = $dbh->do("UPDATE $link dep.base_no SET dep.base_no = 0 WHERE $selector");
	$result = $dbh->do("UPDATE $link dep.range_no SET dep.range_no = 0 WHERE $selector");
	$result = $dbh->do("UPDATE $link dep.color_no SET dep.color_no = 0 WHERE $selector");
	
	$edt->debug_line("UPDATE $link dep.top_no SET dep.top_no = 0 WHERE $selector");
	$edt->debug_line("   [and same for base_no, range_no, color_no]\n");
	
	return;
    }
    
    # Otherwise, then set a caution and return true.
    
    else
    {
	$edt->add_condition('C_BREAK_DEPENDENCIES');
	return 1;
    }
}


# define_intervals ( action, table, record )
# 
# For every interval name defined in this timescale, copy the interval definition to the intervals
# table, unless that interval is already defined by a timescale of higher priority. This will involve
# some combination of creating new intervals or updating existing ones.
# 
# If the record includes the key 'preview' with a true value, then return a list of the interval
# records that would be created, but do not actually create them.

sub define_intervals_action {
    
    my ($edt, $action, $table, $record) = @_;
    
    my $dbh = $edt->dbh;
    my $timescale_id = $record->{timescale_id};
    
    my $sql;
    
    # First make sure that we have a valid timescale_no.
    
    my $timescale_no = $edt->check_timescale_id($timescale_id) || return;

    # If this timescale has errors, this operations must fail.

    my ($error_flag) = $dbh->selectrow_array("
	SELECT has_error FROM $TABLE{TIMESCALE_DATA}
	WHERE timescale_no in ($timescale_no)");

    if ( $error_flag )
    {
	$edt->add_condition('E_TIMESCALE_HAS_ERRORS', $timescale_no);
	return;
    }
    
    # If the 'preview' flag was specified, we query to find what would be done if the full
    # operation were to be carried out.
    
    if ( $record->{preview} )
    {
	# First list out all of the intervals that would be updated or inserted.
	
	$sql = "
	SELECT tsi.interval_no, tsb.timescale_no, tsb.interval_name, tsb.bound_no,
		tsb.age as early_age, tsb.age_prec as early_age_prec,
		upper.age as late_age, upper.age_prec as late_age_prec,
		tsb.interval_type, tsb.color, ts.reference_no, ts.priority
	FROM $TABLE{TIMESCALE_BOUNDS} as tsb
		left join $TABLE{TIMESCALE_INTS} as tsi using (interval_name) 
		join $TABLE{TIMESCALE_BOUNDS} as upper on upper.bound_no = tsb.top_no
		join $TABLE{TIMESCALE_DATA} as ts on ts.timescale_no = tsb.timescale_no
	WHERE tsb.timescale_no in ($timescale_no) and tsb.interval_name <> '' and 
		(tsi.interval_no is null or ts.priority >= tsi.priority)
	ORDER BY tsb.timescale_no, upper.age\n";
	
	$edt->debug_line($sql);
	
	my $preview_list = $dbh->selectall_arrayref($sql, { Slice => { } });

	if ( ref $preview_list eq 'ARRAY' && @$preview_list )
	{
	    foreach my $r ( @$preview_list )
	    {
		$r->{status} = $r->{interval_no} ? 'update' : 'insert';
		push @{$edt->{my_result}}, $r;
	    }
	}
	
	# Then figure out all of the intervals that are currently defined by this timescale but
	# would be deleted because they no longer correspond to a bound in this timescale.
	
	$sql = "
	SELECT tsi.timescale_no, tsi.interval_name, tsi.interval_no,
		tsi.early_age, tsi.early_age_prec, tsi.late_age, tsi.late_age_prec,
		tsi.interval_type, tsi.reference_no, tsi.color, tsi.priority
	FROM $TABLE{TIMESCALE_INTS} as tsi
		left join $TABLE{TIMESCALE_BOUNDS} as tsb using (interval_name, timescale_no) 
	WHERE tsi.timescale_no in ($timescale_no) and tsb.timescale_no is null
	ORDER BY tsi.timescale_no, tsi.late_age\n";
	
	my $delete_list = $dbh->selectall_arrayref($sql, { Slice => { } });
	
	if ( ref $delete_list eq 'ARRAY' && @$delete_list )
	{
	    foreach my $r ( @$delete_list )
	    {
		$r->{status} = 'delete';
		push @{$edt->{my_result}}, $r;
	    }
	}
	
	return;
    }
    
    # Otherwise, start the transaction if it hasn't already been started.
    
    $edt->start_execution;

    # Update all of the matching interval definitions that are not already defined by a
    # higher-priority timescale.
    
    $sql = "
	UPDATE $TABLE{TIMESCALE_INTS} as tsi
		join $TABLE{TIMESCALE_BOUNDS} as tsb using (interval_name)
		join $TABLE{TIMESCALE_BOUNDS} as upper on upper.bound_no = tsb.top_no
		join $TABLE{TIMESCALE_DATA} as ts on ts.timescale_no = tsb.timescale_no
	SET tsi.timescale_no = tsb.timescale_no,
	    tsi.bound_no = tsb.bound_no,
	    tsi.color = tsb.color,
	    tsi.early_age = tsb.age,
	    tsi.early_age_prec = tsb.age_prec,
	    tsi.late_age = upper.age,
	    tsi.late_age_prec = upper.age_prec,
	    tsi.interval_type = tsb.interval_type,
	    tsi.reference_no = ts.reference_no,
	    tsi.priority = ts.priority
	WHERE tsb.timescale_no in ($timescale_no) and
              (tsb.timescale_no = tsi.timescale_no or ts.priority >= tsi.priority)\n";
    
    $edt->debug_line($sql);
    
    my $update_count = $dbh->do($sql);
    
    # Then insert new intervals corresponding to any interval names that don't already appear in
    # the table.
    
    $sql = "
	INSERT INTO $TABLE{TIMESCALE_INTS} (timescale_no, bound_no, interval_name, color,
		early_age, early_age_prec, late_age, late_age_prec, interval_type,
		reference_no, priority)
	SELECT tsb.timescale_no, tsb.bound_no, tsb.interval_name, tsb.color,
		tsb.age, tsb.age_prec, upper.age, upper.age_prec, tsb.interval_type,
		ts.reference_no, ts.priority
	FROM $TABLE{TIMESCALE_BOUNDS} as tsb
		left join $TABLE{TIMESCALE_INTS} as tsi using (interval_name) 
		join $TABLE{TIMESCALE_BOUNDS} as upper on upper.bound_no = tsb.top_no
		join $TABLE{TIMESCALE_DATA} as ts on ts.timescale_no = tsb.timescale_no
	WHERE tsb.timescale_no in ($timescale_no) and tsi.interval_no is null\n";
    
    $edt->debug_line($sql);
    
    my $insert_count = $dbh->do($sql);

    # Now query and return all interval records that are defined from this timescale, plus those
    # that will be deleted because they no longer do. We need to do the query before the deletion,
    # but the deletion will be the next step.
    
    $sql = "
	SELECT tsi.interval_no, tsi.timescale_no, tsi.interval_name, tsb.bound_no,
		tsi.early_age, tsi.early_age_prec, tsi.late_age, tsi.late_age_prec,
		tsi.interval_type, tsi.reference_no, tsi.color, tsi.priority,
		tsb.timescale_no as is_defined
	FROM $TABLE{TIMESCALE_INTS} as tsi
		left join $TABLE{TIMESCALE_BOUNDS} as tsb using (interval_name, timescale_no) 
	WHERE tsi.timescale_no in ($timescale_no)
	ORDER BY tsi.timescale_no, tsi.late_age\n";
    
    $edt->debug_line($sql);

    my $result_list = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    if ( ref $result_list eq 'ARRAY' && @$result_list )
    {
	foreach my $r ( @$result_list )
	{
	    $r->{status} = $r->{is_defined} ? 'defined' : 'deleted';
	    push @{$edt->{my_result}}, $r;
	}
    }
    
    # Finally, delete all of those intervals that were formerly defined by this timescale and no
    # longer are.
    
    $sql = "
	DELETE $TABLE{TIMESCALE_INTS}
	FROM $TABLE{TIMESCALE_INTS} 
		left join $TABLE{TIMESCALE_BOUNDS} using (interval_name, timescale_no)
	WHERE $TABLE{TIMESCALE_INTS}.timescale_no in ($timescale_no) and $TABLE{TIMESCALE_BOUNDS}.timescale_no is null\n";
    
    $edt->debug_line($sql);

    my $delete_count = $dbh->do($sql);
}


sub undefine_intervals_action {

    my ($edt, $action, $table, $record) = @_;
    
    my $dbh = $edt->dbh;
    my $timescale_id = $record->{timescale_id};
    
    my $sql;
    
    # First make sure that we have a valid timescale_no.
    
    my $timescale_no = $edt->check_timescale_id($timescale_id) || return;

    # If this timescale has errors, this operation is still possible.

    # First figure out all of the intervals that are currently defined by this timescale and thus
    # would be deleted.

    $sql = "
	SELECT tsi.timescale_no, tsi.interval_name, tsi.interval_no,
		tsi.early_age, tsi.early_age_prec, tsi.late_age, tsi.late_age_prec,
		tsi.interval_type, tsi.reference_no, tsi.color, tsi.priority
	FROM $TABLE{TIMESCALE_INTS} as tsi
	WHERE tsi.timescale_no in ($timescale_no)
	ORDER BY tsi.timescale_no, tsi.late_age\n";
    
    my $delete_list = $dbh->selectall_arrayref($sql, { Slice => { } });
	
    if ( ref $delete_list eq 'ARRAY' && @$delete_list )
    {
	foreach my $r ( @$delete_list )
	{
	    $r->{status} = 'delete';
	    push @{$edt->{my_result}}, $r;
	}
    }

    # If the 'preview' flag was specified, then just return this list (via my_result above).
    
    if ( $record->{preview} )
    {
	return;
    }
    
    # Otherwise, start the transaction if it hasn't already been started.
    
    $edt->start_execution;

    # Then actually delete all of the intervals defined by this timescale.
    
    $sql = "
	DELETE FROM $TABLE{TIMESCALE_INTS} 
	WHERE $TABLE{TIMESCALE_INTS}.timescale_no in ($timescale_no)\n";
    
    $edt->debug_line($sql);
    
    my $delete_count = $dbh->do($sql);
}
    
    # # $$$    # First get a list of the interval names that are defined by this timescale.

    # $sql = "
    # 	SELECT tsb.bound_no, tsb.timescale_no, tsi.interval_name, tsi.early_age, tsi.early_age_prec,
    # 		tsi.late_age, tsi.late_age_prec, tsi.interval_type, tsi.color, tsi.priority, tsi.reference_no
    # 	FROM $TABLE{TIMESCALE_BOUNDS} as tsb
    # 		join $TABLE{TIMESCALE_INTS} as tsi using (interval_name, timescale_no)
    # 	WHERE tsb.timescale_no in ($timescale_no)\n";
    
    # $edt->debug_line($sql);

    # my $interval_list = $dbh->selectall_arrayref($sql, { Slice => { } });

    # $interval_list ||= [ ];
    
    # my $interval_count = scalar(@$interval_list);
    
    # # If there are none, then return.

    # return $interval_count unless $interval_count;

    # # Otherwise, list any definitions for the matching intervals that occur in other timescales.
    
    # $sql = "
    # 	SELECT tsi.interval_no, tsi.interval_name, other.bound_no, other.timescale_no, 
    # 		other.age as early_age,	other.age_prec as early_age_prec,
    # 		upper.age as late_age, upper.age_prec as late_age_prec,
    # 		other.interval_type, other.color, otherts.priority, otherts.reference_no
    # 	FROM $TABLE{TIMESCALE_BOUNDS} as tsb
    # 		join $TABLE{TIMESCALE_INTS} as tsi using (interval_name, timescale_no)
    # 		join $TABLE{TIMESCALE_BOUNDS} as other on other.interval_name = tsi.interval_name and 
    # 			other.timescale_no <> tsb.timescale_no
    # 		join $TABLE{TIMESCALE_BOUNDS} as upper on upper.bound_no = other.top_no
    # 		join $TABLE{TIMESCALE_DATA} as otherts on otherts.timescale_no = other.timescale_no
    # 	WHERE tsb.timescale_no in ($timescale_no)
    # 	ORDER BY other.timescale_no\n";

    # $edt->debug_line($sql);

    # my $other_list = $dbh->selectall_arrayref($sql, { Slice => { } });

    # $other_list ||= [ ];
    
    # # Now collect up the highest priority definition for each of those other intervals, listed by
    # # interval name. In the case of priority ties, the lowest timescale_no wins.
    
    # my %definition;
    
    # foreach my $r ( @$other_list )
    # {
    # 	my $interval_name = $r->{interval_name};
	
    # 	if ( ! $definition{$interval_name} )
    # 	{
    # 	    $definition{$interval_name} = $r;
    # 	}
	
    # 	elsif ( defined $r->{priority} && defined $definition{$interval_name}{priority} &&
    # 	     $r->{priority} > $definition{$interval_name}{priority} )
    # 	{
    # 	    $definition{$interval_name} = $r;
    # 	}
    # }
    
    # # For each of these definitions, execute an update statement that will update the
    # # corresponding interval record.
    
    # foreach my $r ( values %definition )
    # {
    # 	my $interval_no = $dbh->quote($r->{interval_no});
    # 	my $timescale_no = $dbh->quote($r->{timescale_no});
    # 	my $bound_no = $dbh->quote($r->{bound_no});
    # 	my $early_age = $dbh->quote($r->{early_age});
    # 	my $early_age_prec = defined $r->{early_age_prec} ? $dbh->quote($r->{early_age_prec}) : 'NULL';
    # 	my $late_age = $dbh->quote($r->{late_age});
    # 	my $late_age_prec = defined $r->{late_age_prec} ? $dbh->quote($r->{late_age_prec}) : 'NULL';
    # 	my $color = $r->{color} ? $dbh->quote($r->{color}) : "''";
    # 	my $reference_no = $r->{reference_no} ? $dbh->quote($r->{reference_no}) : 'NULL';
    # 	my $priority = defined $r->{priority} ? $dbh->quote($r->{priority}) : 'NULL';
	
    # 	my $sql = "UPDATE $TABLE{TIMESCALE_INTS}
    # 		SET timescale_no = $timescale_no,
    # 		    bound_no = $bound_no,
    # 		    early_age = $early_age, early_age_prec = $early_age_prec,
    # 		    late_age = $late_age, late_age_prec = $late_age_prec,
    # 		    color = $color, reference_no = $reference_no, priority = $priority
    # 		WHERE interval_no = $interval_no\n";

    # 	my $result = $dbh->do($sql);

    # 	# $$$
    # }
    
    # # Then get a list of the interval names that are not mentioned in any other timescale that has
    # # a non-zero priority.
    
    # $sql = "
    # 	SELECT tsb.bound_no, tsb.timescale_no, tsi.interval_name, tsi.early_age, tsi.early_age_prec,
    # 		tsi.late_age, tsi.late_age_prec, tsi.interval_type, tsi.color, tsi.priority, tsi.reference_no
    # 	FROM $TABLE{TIMESCALE_BOUNDS} as tsb
    # 		join $TABLE{TIMESCALE_INTS} as tsi using (interval_name, timescale_no)
    # 		left join $TABLE{TIMESCALE_BOUNDS} as other on other.interval_name = tsi.interval_name and 
    # 			other.timescale_no <> tsb.timescale_no and other.top_no <> 0
    # 		join $TABLE{TIMESCALE_DATA} as otherts on otherts.timescale_no = other.timescale_no
    # 	WHERE (other.bound_no is null or otherts.priority = 0) and tsb.timescale_no in ($timescale_no)\n";
    
    # $edt->debug_line($sql);
    
    # my $check_list = $dbh->selectall_arrayref($sql, { Slice => { } });
    
    # $check_list ||= [ ];
    
    # my $check_count = scalar(@$check_list);
    
    # # If we are being asked only to check, or if there is nothing to update, then return this
    # # list.
    
    # if ( $preview || $check_count == 0 )
    # {
    # 	return $check_list;
    # }
    
    # # Otherwise, start the transaction if it hasn't already been started.
    
    # $edt->start_execution;
    
    # # Then select all matching definitions in other timescales that have non-zero priority.
    
    # $sql = "
    # 	SELECT other.*, otherts.priority
    # 	FROM $TABLE{TIMESCALE_BOUNDS} as tsb
    # 		join $TABLE{TIMESCALE_INTS} as tsi using (interval_name, timescale_no)
    # 		join $TABLE{TIMESCALE_BOUNDS} as other on other.interval_name = tsi.interval_name and 
    # 			other.timescale_no <> tsb.timescale_no and other.top_no <> 0
    # 		join $TABLE{TIMESCALE_DATA} as otherts on otherts.timescale_no = other.timescale_no
    # 	WHERE tsb.timescale_no in ($timescale_no) and otherts.priority > 0\n";

    # $edt->debug_line($sql);

    # my $matches = $dbh->selectall_arrayref($sql, { Slice => { } });

    
    

    # $sql = "
    # 	DELETE $TABLE{TIMESCALE_INTS}
    # 	FROM $TABLE{TIMESCALE_BOUNDS}
    # 		join $TABLE{TIMESCALE_INTS} using (interval_name, timescale_no)
    # 		left join $TABLE{TIMESCALE_BOUNDS} as other on other.interval_name = 
    # 			$TABLE{TIMESCALE_INTS}.interval_name and other.timescale_no <> tsb.timescale_no and
    # 			other.top_no <> 0
    # 		join $TABLE{TIMESCALE_DATA} as otherts on otherts.timescale_no = other.timescale_no
    # 	WHERE (other.bound_no is null or otherts.priority = 0) and tsb.timescale_no in ($timescale_no)\n";

    # $edt->debug_line($sql);

    # my $delete_count = $dbh->do($sql);
    


sub check_timescale_id {

    my ($edt, $timescale_id) = @_;
    
    if ( $timescale_id && (ref $timescale_id eq 'PBDB::ExtIdent' ||
			   $timescale_id =~ /^\d+$/) )
    {
	return $timescale_id + 0;
    }
    
    elsif ( $timescale_id && $timescale_id =~ $IDRE{TSC} )
    {
	return $1;
    }
    
    elsif ( $timescale_id && $timescale_id =~ $IDRE{LOOSE} )
    {
	$edt->add_condition('E_EXTTYPE', 'timescale_id', "external identifier must be of type '$IDP{TSC}', was '$1'");
	return;
    }
    
    else
    {
	$edt->add_condition('E_PARAM', "invalid timescale identifier '$timescale_id'");
	return;
    }
}


# complete_table_definition ( dbh, table_specifier, arg, debug )
#
# This method is used to define the triggers necessary for the TIMESCALE_BOUNDS table to functio
# properly. It should be called from any test file that establishes the timescale tables in the
# test database. It could also be called when the timescale tables in the main database are
# established, in order to create the necessary triggers for them.

sub complete_table_definition {

    my ($class, $dbh, $table_specifier, $arg, $debug) = @_;

    croak "table specifier must be 'TIMESCALE_BOUNDS'" unless $table_specifier eq 'TIMESCALE_BOUNDS';
    croak "table 'TIMESCALE_BOUNDS' does not exist" unless $TABLE{TIMESCALE_BOUNDS};
    croak "table 'TIMESCALE_DATA' does not exist" unless $TABLE{TIMESCALE_DATA};
    
    my $dbstring = '';
    
    if ( $TABLE{TIMESCALE_BOUNDS} =~ /^([^.]+[.])/ )
    {
	$dbstring = $1;
    }
    
    # Now create or replace the necessary triggers.
    
    $dbh->do("CREATE OR REPLACE TRIGGER ${dbstring}insert_bound
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
    
    $dbh->do("CREATE OR REPLACE TRIGGER ${dbstring}update_bound
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
    
    &TableDefs::debug_line("Established triggers on $TABLE{TIMESCALE_BOUNDS}", $debug) if $debug;
    
    return 1;
}


1;
