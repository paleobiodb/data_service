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


    # my $dbh = $edt->dbh;
    # my $keylist = $edt->get_keylist($action);
    # my $result;
    
    # # If we don't have an actual timescale_no, there is no point in continuing with a delete operation.
    
    # unless ( $keylist )
    # {
    # 	$edt->add_condition('E_NO_KEY', 'delete');
    # 	return;
    # }
    
    # # Now we query for any bounds *in other timescales* that depend on this one.
    
    # my $keyexpr = "timescale_no in ($keylist)";
    
    # my $link = "$TABLE{TIMESCALE_BOUNDS} as base JOIN $TABLE{TIMESCALE_BOUNDS} as dep on base.timescale_no <> dep.timescale_no and base.bound_no =";
    
    
    # # If we find any, then we must either return a caution or break the dependencies.
    
    # if ( $has_deps )
    # {
    # 	unless ( $edt->allows('BREAK_DEPENDENCIES') )
    # 	{
    # 	    $edt->add_condition('C_BREAK_DEPENDENCIES');
    # 	    return;
    # 	}
	
    # 	my $result = $dbh->do("UPDATE $link dep.top_no WHERE base.$keyexpr SET dep.top_no = 0");
    # 	my $result = $dbh->do("UPDATE $link dep.base_no WHERE base.$keyexpr SET dep.base_no = 0");
    # 	my $result = $dbh->do("UPDATE $link dep.range_no WHERE base.$keyexpr SET dep.range_no = 0");
    # 	my $result = $dbh->do("UPDATE $link dep.color_no WHERE base.$keyexpr SET dep.color_no = 0");
    # 	my $result = $dbh->do("UPDATE $link dep.refsource_no WHERE base.$keyexpr SET dep.refsource_no = 0");
    # }
    
    # # If any intervals have this timescale as their authority, add them to the list of intervals
    # # to recompute at the end of the transaction.
    
    # my ($auth) = $dbh->selectcol_arrayref("SELECT interval_no FROM $TABLE{TIMESCALE_INTS}
    # 	WHERE authority_timescale_no in ($keylist)");
    
    # if ( ref $auth eq 'ARRAY' && @$auth )
    # {
    # 	foreach my $i ( @$auth )
    # 	{
    # 	    $edt->set_attr_key('update_intervals', $i, 1);
    # 	}
    # }
    
    # # Now we must delete all of the bounds in the timescale.
    
    # $result = $dbh->do("DELETE FROM $TABLE{TIMESCALE_BOUNDS} WHERE $keyexpr");
    
    # # And finally, if any other timescale uses this one as its source_no, set that to zero.
    
    # $result = $dbh->do("UPDATE $TABLE{TIMESCALE_DATA} SET source_timescale_no = 0
    # 	WHERE source_timescale_no in ($keylist)");
#   }


# sub add_timescale {
    
#     my ($edt, $attrs) = @_;
    
#     croak "add_timescale: bad attrs\n" unless ref $attrs eq 'HASH';
#     croak "add_timescale: must not have a value for timescale_id\n" if $attrs->{timescale_id};
    
#     my $dbh = $edt->dbh;
    
#     # Start by making sure that we are in a state in which we can proceed.
    
#     return 0 unless $edt->can_check;
    
#     # Then make sure that we can actually add records.
    
#     unless ( $edt->{condition}{CREATE_RECORDS} )
#     {
# 	$edt->add_condition("C_CREATE_RECORDS", $attrs->{record_label});
# 	return;
#     }
    
#     # Now check that all of the specified attributes are of the correct type and in the correct
#     # value range, and that all references to other records match up to existing records.
    
#     $edt->new_record;
    
#     my ($fields, $values) = $edt->check_timescale_attrs('add', 'timescale', $attrs);
    
#     # Then make sure that the necessary attributes have the proper values.
    
#     $edt->check_timescale_values($attrs);
    
#     # If any errors occurred, we stop here. This counts as "check only" mode.
    
#     return 0 unless $edt->can_edit;
    
#     # Otherwise, insert the new record.
    
#     my $field_list = join(',', @$fields);
#     my $value_list = join(',', @$values);
    
#     my $sql = "INSERT INTO $TIMESCALE_DATA ($field_list) VALUES ($value_list)";
#     my ($insert_result, $insert_id);
    
#     print STDERR "$sql\n\n" if $edt->debug;
    
#     try {
# 	$insert_result = $dbh->do($sql);
# 	$insert_id = $dbh->last_insert_id(undef, undef, undef, undef);
# 	print STDERR "RESULT: 0\n" if $edt->debug && ! $insert_id;
#     }
	
#     catch {
# 	print STDERR "ERROR: $_\n";
#     };
    
#     if ( $insert_id )
#     {
# 	$edt->{timescale_updated}{$insert_id} = 1;
# 	return $insert_id;
#     }
    
#     else
#     {
# 	$edt->add_condition("E_INTERNAL", $attrs->{record_label});
# 	return 0;
#     }
# }


# sub update_timescale {

#     my ($edt, $attrs) = @_;
    
#     croak "update_timescale: bad attrs\n" unless ref $attrs eq 'HASH';
#     croak "update_timescale: must have a value for timescale_id\n" unless $attrs->{timescale_id};
    
#     my $dbh = $edt->dbh;
    
#     # Start by making sure that we are in a state in which we can proceed.
    
#     return 0 unless $edt->can_check;
    
#     # We first need to make sure that the record to be updated actually exists, and fetch its
#     # current attributes.
    
#     my $timescale_id = $attrs->{timescale_id};
#     my $record_label = $attrs->{record_label} || $attrs->{timescale_id};
    
#     unless ( $timescale_id =~ /^\d+$/ && $timescale_id > 0 )
#     {
# 	$edt->add_condition("E_TIMESCALE_ID", $record_label, $timescale_id);
# 	return 0;
#     }
    
#     my ($current) = $dbh->selectrow_hashref("
# 		SELECT * FROM $TIMESCALE_DATA WHERE timescale_no = $timescale_id");
    
#     unless ( $current )
#     {
# 	$edt->add_condition("E_NOT_FOUND", $record_label, $timescale_id);
# 	return 0;
#     }
    
#     # Now check that all of the specified attributes are of the correct type and in the correct
#     # value range, and that all references to other records match up to existing records.
    
#     $edt->new_record;
    
#     my ($fields, $values) = $edt->check_timescale_attrs('update', 'timescale', $attrs);
    
#     # Then make sure that the necessary attributes have the proper values.
    
#     $edt->check_timescale_values($attrs, $current);
    
#     # If any errors occurred, we stop here. This counts as "check only" mode.
    
#     return 0 unless $edt->can_edit;
    
#     # Otherwise, update the record.
    
#     my $set_list = $edt->generate_set_list($fields, $values);
    
#     return 0 unless $set_list;
    
#     my $sql = "	UPDATE $TIMESCALE_DATA SET $set_list, modified = NOW()
# 		WHERE timescale_no = $timescale_id";
#     my $update_result;
    
#     print STDERR "$sql\n\n" if $edt->debug;
    
#     try {
# 	$update_result = $dbh->do($sql);
# 	print STDERR "RESULT: 0\n" if $edt->debug && ! $update_result;
#     }
	
#     catch {
# 	print STDERR "ERROR: $_\n";
#     };
    
#     if ( $update_result )
#     {
# 	$edt->{timescale_updated}{$timescale_id} = 1;
# 	return $timescale_id;
#     }
    
#     else
#     {
# 	$edt->add_condition("E_INTERNAL", $record_label, $timescale_id);
# 	return 0;
#     }
# }


# sub delete_timescale {

#     my ($edt, $timescale_id) = @_;
    
#     return unless defined $timescale_id && $timescale_id ne '';
    
#     # Throw an exception if we are given an invalid id.
    
#     croak "delete_timescale: bad timescale identifier '$timescale_id'\n" unless $timescale_id =~ /^\d+$/;
    
#     # Then make sure that we are in a state in which we can proceed.
    
#     return 0 unless $edt->can_check;
    
#     my $dbh = $edt->dbh;
    
#     # First determine if the record actually exists.
    
#     my ($exists) = $dbh->selectrow_array("
# 		SELECT timescale_no FROM $TIMESCALE_DATA WHERE timescale_no = $timescale_id");
    
#     unless ( $exists )
#     {
# 	$edt->add_condition("W_NOT_FOUND", $timescale_id);
# 	return 0;
#     }
    
#     # If we get here, then there is a record in the database that we can delete.
    
#     # Permission checks go here.
    
#     # ... permission checks ...
    
#     # If any errors occurred, or if $check_only was specified, we stop here.
    
#     return 0 unless $edt->can_edit;
    
#     # Now check to see if there are any other boundaries that depend on the ones in this
#     # timescale. If so, then we need to deal with them. If the condition BREAK_DEPENDENCIES is
#     # allowed, then cut each of these records loose. Otherwise, return a caution.
    
#     my $sql;
    
#     $sql = "	SELECT count(*) FROM $TIMESCALE_BOUNDS as related
# 			join $TIMESCALE_BOUNDS as base
# 		WHERE base.timescale_no in ($timescale_id) and
# 		    related.base_no = base.bound_no or related.range_no = base.bound_no or
# 		    related.color_no = base.bound_no or related.refsource_no = base.bound_no";
    
#     print STDERR "$sql\n\n" if $edt->debug;
    
#     my ($dependent_count) = $dbh->selectrow_array($sql);
#     my $result;
    
#     if ( $dependent_count )
#     {
# 	unless ( $edt->{condition}{BREAK_DEPENDENCIES} )
# 	{
# 	    $edt->add_condition("C_BREAK_DEPENDENCIES", $timescale_id, $dependent_count);
# 	    return 0;
# 	}
	
# 	else
# 	{
# 	    $result = $edt->detach_related_bounds('timescale', $timescale_id);
# 	    return 0 unless $result;
# 	}
#     }
    
#     # If we get here, then we can delete. We return an OK as long as no exception is caught, on
#     # the assumption that the delete statement is so simple that the only way it could go wrong is
#     # if the record is somehow already gone.
    
#     my @sql = "	DELETE FROM $TIMESCALE_BOUNDS WHERE timescale_no in ($timescale_id)";
    
#     push @sql,"	DELETE FROM $TIMESCALE_DATA WHERE timescale_no in ($timescale_id)";
    
#     my $delete_result;
    
#     try {
	
# 	foreach my $stmt ( @sql )
# 	{
# 	    print STDERR "$stmt\n\n" if $edt->debug;
# 	    $delete_result = $dbh->do($sql);
# 	}	
	
# 	print STDERR "RESULT: 0\n" if $edt->debug && ! $delete_result;
#     }
    
#     catch {
# 	print STDERR "ERROR: $_\n";
#     };
    
#     if ( $delete_result )
#     {
# 	$edt->{timescale_updated}{$timescale_id} = 1;
# 	return $timescale_id;
#     }
    
#     else
#     {
# 	$edt->add_condition("W_INTERNAL", $timescale_id);
# 	return 0;
#     }
# }


# # Add a new boundary according to the specified attributes.

# sub add_boundary {
    
#     my ($edt, $attrs) = @_;
    
#     croak "add_boundary: bad attrs\n" unless ref $attrs eq 'HASH';
#     croak "add_boundary: must not have a value for bound_id\n" if $attrs->{bound_id};
    
#     # Start by making sure that we are in a state in which we can proceed.
    
#     return 0 unless $edt->can_check;
    
#     my $dbh = $edt->dbh;
    
#     # Make sure that we know what timescale to create the boundary in, and
#     # that a bound type was specified.
    
#     unless ( $attrs->{timescale_id} )
#     {
#         $edt->add_condition("E_BOUND_TIMESCALE", $attrs->{record_label});
#     }
    
#     unless ( $attrs->{bound_type} )
#     {
# 	$edt->add_condition("E_BOUND_TYPE", $attrs->{record_label});
#     }
    
#     # Then check for missing or redundant attributes. These will vary by bound type.
    
#     my $timescale_id = $attrs->{timescale_id};
#     my $bound_type = $attrs->{bound_type};
    
#     if ( $bound_type eq 'absolute' || $bound_type eq 'spike' )
#     {
# 	$edt->add_condition("E_AGE_MISSING", $attrs->{record_label})
# 	    unless $attrs->{age};

# 	$edt->add_condition("W_BASE_IGNORED", $attrs->{record_label})
# 	    if $attrs->{range_id};
	
# 	$edt->add_condition("W_RANGE_IGNORED", $attrs->{record_label})
# 	    if $attrs->{range_id};
	
# 	$edt->add_condition("W_OFFSET_IGNORED", $attrs->{record_label})
# 	    if $attrs->{offset};
#     }
    
#     elsif ( $bound_type eq 'same' )
#     {
# 	$edt->add_condition("E_BASE_MISSING", $attrs->{record_label})
# 	    unless $attrs->{base_id};
	
# 	$edt->add_condition("W_RANGE_IGNORED", $attrs->{record_label})
# 	    if $attrs->{range_id};
	
# 	$edt->add_condition("W_AGE_IGNORED", $attrs->{record_label})
# 	    if $attrs->{age};

# 	$edt->add_condition("W_OFFSET_IGNORED", $attrs->{record_label})
# 	    if $attrs->{offset};
#     }
    
#     elsif ( $bound_type eq 'offset' || $bound_type eq 'percent' )
#     {
# 	$edt->add_condition("E_BASE_MISSING", $attrs->{record_label})
# 	    unless $attrs->{base_id};
	
# 	$edt->add_condition("E_OFFSET_MISSING", $attrs->{record_label})
# 	    unless $attrs->{offset};
	
# 	$edt->add_condition("W_AGE_IGNORED", $attrs->{record_label})
# 	    if $attrs->{age};
	
# 	if ( $bound_type eq 'percent' )
# 	{
# 	    $edt->add_condition("E_RANGE_MISSING", $attrs->{record_label})
# 		unless $attrs->{range_id};
# 	}
	
# 	else
# 	{
# 	    $edt->add_condition("W_RANGE_IGNORED", $attrs->{record_label})
# 		if $attrs->{range_id};
# 	}
#     }
    
#     # Now check that all of the specified attributes are of the correct type and in the correct
#     # value range, and that all references to other records match up to existing records.
    
#     my ($fields, $values) = $edt->check_timescale_attrs('add', 'bound', $attrs);
    
#     # Then make sure that the necessary attributes have the proper values.
    
#     $edt->check_bound_values($attrs);
    
#     # If any errors occurred, we stop here. This counts as "check only" mode.
    
#     return 0 unless $edt->can_edit;
    
#     # Otherwise, insert the new record.
    
#     my $field_list = join(',', @$fields, 'is_updated');
#     my $value_list = join(',', @$values, '1');
    
#     my $sql = "INSERT INTO $TIMESCALE_BOUNDS ($field_list) VALUES ($value_list)";
#     my ($insert_result, $insert_id);
    
#     print STDERR "$sql\n\n" if $edt->debug;
    
#     try {
# 	$insert_result = $dbh->do($sql);
# 	$insert_id = $dbh->last_insert_id(undef, undef, undef, undef);
# 	print STDERR "RESULT: 0\n" if $edt->debug && ! $insert_id;
#     }
    
#     catch {
# 	print STDERR "ERROR: $_\n";
#     };
    
#     if ( $insert_id )
#     {
# 	$edt->{bound_updated}{$insert_id} = 1;
# 	$edt->{timescale_updated}{$timescale_id} = 1;
# 	return $insert_id;
#     }
    
#     else
#     {
# 	$edt->add_condition("E_INTERNAL", $attrs->{record_label});
# 	return 0;
#     }
# }


# sub update_boundary {

#     my ($edt, $attrs) = @_;
    
#     croak "update_boundary: bad attrs\n" unless ref $attrs eq 'HASH';
#     croak "update_boundary: must have a value for bound_id\n" unless $attrs->{bound_id};
    
#     my $dbh = $edt->dbh;
    
#     my $bound_id = $attrs->{bound_id};
#     my $record_label = $attrs->{record_label} || $attrs->{bound_id};
    
#     # Start by making sure that we are in a state in which we can proceed.
    
#     return 0 unless $edt->can_check;
    
#     # We first need to make sure that the record to be updated actually exists, and fetch its
#     # current attributes.
    
#     unless ( $bound_id =~ /^\d+$/ && $bound_id > 0 )
#     {
# 	$edt->add_condition("E_BOUND_ID", $record_label, $bound_id);
# 	return 0;
#     }
    
#     my ($current) = $dbh->selectrow_hashref("
# 		SELECT * FROM $TIMESCALE_BOUNDS WHERE bound_no = $bound_id");
    
#     unless ( $current )
#     {
# 	$edt->add_condition("E_NOT_FOUND", $record_label, $bound_id);
# 	return 0;
#     }
    
#     # If a timescale_id was specified, it must match the current one otherwise an error will be
#     # thrown. It is not permitted to move a boundary to a different timescale.
    
#     if ( defined $attrs->{timescale_id} && $attrs->{timescale_id} ne '' )
#     {
# 	if ( $current->{timescale_no} && $current->{timescale_no} ne $attrs->{timescale_id} )
# 	{
# 	    $edt->add_condition("E_BOUND_TIMESCALE", $record_label);
# 	}
#     }
    
#     # Check for missing or redundant attributes. These will vary by bound type.
    
#     my $bound_type = $attrs->{bound_type} || $current->{bound_type};
#     my $timescale_id = $current->{timescale_no};
    
#     if ( $bound_type eq 'absolute' || $bound_type eq 'spike' )
#     {
# 	$edt->add_condition("E_AGE_MISSING", $record_label)
# 	    unless defined $attrs->{age} || defined $current->{age};
	
# 	$edt->add_condition("W_BASE_IGNORED", $record_label)
# 	    if $attrs->{range_id};
	
# 	$edt->add_condition("W_RANGE_IGNORED", $record_label)
# 	    if $attrs->{range_id};
	
# 	$edt->add_condition("W_OFFSET_IGNORED", $record_label)
# 	    if $attrs->{offset};
#     }
    
#     elsif ( $bound_type eq 'same' )
#     {
# 	$edt->add_condition("E_BASE_MISSING", $record_label)
# 	    unless $attrs->{base_id} || $current->{base_no};
	
# 	$edt->add_condition("W_RANGE_IGNORED", $record_label)
# 	    if $attrs->{range_id};
	
# 	$edt->add_condition("W_AGE_IGNORED", $record_label)
# 	    if defined $attrs->{age};

# 	$edt->add_condition("W_OFFSET_IGNORED", $record_label)
# 	    if $attrs->{offset};
#     }
    
#     elsif ( $bound_type eq 'offset' || $bound_type eq 'percent' )
#     {
# 	$edt->add_condition("E_BASE_MISSING", $record_label)
# 	    unless $attrs->{base_id} || $current->{base_no};
	
# 	$edt->add_condition("E_OFFSET_MISSING", $record_label)
# 	    unless $attrs->{offset};
	
# 	$edt->add_condition("W_AGE_IGNORED", $record_label)
# 	    if $attrs->{age};
	
# 	if ( $bound_type eq 'percent' )
# 	{
# 	    $edt->add_condition("E_RANGE_MISSING", $record_label)
# 		unless $attrs->{range_id} || $current->{range_no};
# 	}
	
# 	else
# 	{
# 	    $edt->add_condition("W_RANGE_IGNORED", $record_label)
# 		if $attrs->{range_id};
# 	}
#     }
    
#     # Now check that all of the specified attributes are of the correct type and in the correct
#     # value range, and that all references to other records match up to existing records.
    
#     my ($fields, $values) = $edt->check_timescale_attrs('update', 'bound', $attrs);
    
#     # Then make sure that the necessary attributes have the proper values.
    
#     $edt->check_bound_values($attrs, $current);
    
#     # If any errors occurred, or if $check_only was specified, we stop here.
    
#     return 0 unless $edt->can_edit;
    
#     # Otherwise, update the record.
    
#     my $set_list = $edt->generate_set_list($fields, $values);
    
#     return 0 unless $set_list;
    
#     my $sql = "	UPDATE $TIMESCALE_BOUNDS SET $set_list, is_updated=1, modified = now()
# 		WHERE bound_no = $bound_id";
    
#     print STDERR "$sql\n\n" if $edt->debug;
    
#     my $update_result;
    
#     try {
# 	$update_result = $dbh->do($sql);
# 	print STDERR "RESULT: 0\n" unless $update_result;
#     }
    
#     catch {
# 	print STDERR "ERROR: $_\n";
#     };
    
#     if ( $update_result )
#     {
# 	$edt->{bound_updated}{$bound_id} = 1;
# 	$edt->{timescale_updated}{$timescale_id} = 1;
	
# 	return $bound_id;
#     }
    
#     else
#     {
# 	$edt->add_condition("E_INTERNAL", $record_label);
# 	return 0;
#     }
# }


# sub delete_boundary {

#     my ($edt, $delete_type, $id) = @_;
    
#     return unless defined $id && $id ne '';
    
#     croak "delete_boundary: bad value '$id' for id\n" unless $id =~ /^\d+$/;
    
#     # Start by making sure that we are in a state in which we can proceed.
    
#     return 0 unless $edt->can_check;
    
#     my $dbh = $edt->dbh;
    
#     my $timescale_id;
    
#     # If we are deleting one or more boundaries, we need to make sure the records
#     # actually exist and fetch their current attributes.
    
#     if ( $delete_type eq 'bound' )
#     {
# 	# unless ( $bound_id =~ /^\d+$/ && $bound_id > 0 )
# 	# {
# 	#     return $edt->add_condition("E_BOUND_ID: bad value '$bound_id' for 'bound_id'");
# 	# }
	
# 	my ($current) = $dbh->selectrow_hashref("
# 		SELECT * FROM $TIMESCALE_BOUNDS WHERE bound_no = $id");
	
# 	unless ( $current )
# 	{
# 	    $edt->add_condition("W_NOT_FOUND", $id);
# 	    return 0;
# 	}
	
# 	# If we get here, then there is a record in the database that we can delete. If a
# 	# timescale id was specified, it had better match the one in the record.
	
# 	# if ( defined $attrs->{timescale_id} && $attrs->{timescale_id} ne '' )
# 	# {
# 	#     if ( $current->{timescale_no} && $current->{timescale_no} ne $attrs->{timescale_id} )
# 	#     {
# 	# 	$edt->add_condition("E_BOUND_TIMESCALE: the specified bound is not associated with the specified timescale");
# 	#     }
# 	# }
	
# 	# Keep track of what timescale this bound is in, if we didn't know it originally.
	
# 	$timescale_id = $current->{timescale_id};
#     }
    
#     # If we are given a timescale_id but not a bound_id, check to make sure that timescale
#     # actually exists.
    
#     elsif ( $delete_type ne 'timescale' || $delete_type eq 'unupdated' )
#     {
# 	my ($ts) = $dbh->selectrow_hashref("
# 		SELECT * FROM $TIMESCALE_DATA WHERE timescale_no = $id");
	
# 	unless ( $ts )
# 	{
# 	    return $edt->add_condition("E_NOT_FOUND: timescale '$id' is not in the database");
# 	}
	
# 	$timescale_id = $id;
#     }
    
#     # Otherwise, we weren't given anything to work with.
    
#     else
#     {
# 	croak "delete_boundary: bad value for 'delete_type'";
#     }
    
#     # Permission checks go here.
    
#     # ... permission checks ...
    
#     # If any errors occurred, or if $check_only was specified, we stop here.
    
#     return 0 unless $edt->can_edit;
    
#     # Now check to see if there are any other boundaries that depend on this one. If so, then we
#     # need to deal with them. If the condition BREAK_DEPENDENCIES is allowed, then cut each of these
#     # records loose. Otherwise, return a caution.
    
#     my $sql;
    
#     if ( $delete_type eq 'bound' )
#     {
# 	$sql = "SELECT count(*) FROM $TIMESCALE_BOUNDS
# 		WHERE base_no = $id or range_no = $id or
# 			color_no = $id or refsource_no = $id";
#     }
    
#     else
#     {
# 	my $updated_clause = ''; $updated_clause = "and source.is_updated = 0" if $delete_type = 'unupdated';
	
# 	$sql = "SELECT count(*) FROM $TIMESCALE_BOUNDS as tsb
# 		join $TIMESCALE_BOUNDS as source on tsb.base_no = source.bound_no
# 		or tsb.range_no = source.bound_no or tsb.color_no = source.bound_no
# 		or tsb.refsource_no = source.bound_no
# 		WHERE source.timescale_no = $id $updated_clause";
#     }
    
#     my ($dependent_count) = $dbh->selectrow_hashref($sql);
    
#     if ( $dependent_count )
#     {
# 	print STDERR "$sql\n\n" if $edt->debug;
	
# 	unless ( $edt->{condition}{BREAK_DEPENDENCIES} )
# 	{
# 	    return $edt->add_condition("C_BREAK_DEPENDENCIES: there are $dependent_count other bounds that depend on the bound or bounds to be deleted");
# 	}
	
# 	my $result;
	
# 	$result = $edt->detach_related_bounds($delete_type, $id);
#     }
    
#     # If we get here, then we can delete. We return an OK as long as no exception is caught, on
#     # the assumption that the delete statement is so simple that the only way it could go wrong is
#     # if the record is somehow already gone.
    
#     if ( $delete_type eq 'bound' )
#     {
# 	$sql = " DELETE FROM $TIMESCALE_BOUNDS WHERE bound_no = $id";
#     }
    
#     elsif ( $delete_type eq 'timescale' )
#     {
# 	$sql = " DELETE FROM $TIMESCALE_BOUNDS WHERE timescale_no = $id";
#     }
    
#     elsif ( $delete_type eq 'unupdated' )
#     {
# 	$sql = " DELETE FROM $TIMESCALE_BOUNDS
# 		WHERE timescale_no = $id and is_updated = 0";
#     }
    
#     else
#     {
# 	croak "delete_boundary: bad value '$delete_type' for 'delete_type'";
#     }
    
#     print STDERR "$sql\n\n" if $edt->debug;
    
#     my $delete_result;
    
#     try {
# 	$delete_result = $dbh->do($sql);
# 	print STDERR "RESULT: 0\n" unless $delete_result;
#     }
    
#     catch {
# 	print STDERR "ERROR: $_\n";
#     };
    
#     if ( $delete_result )
#     {
# 	$edt->{timescale_updated} = $timescale_id;
# 	return $id;
#     }
    
#     else
#     {
# 	$edt->add_condition("W_INTERNAL: an error occurred while deleting record '$id'") unless $delete_result;
# 	return 0;
#     }
# }


# # detach_related_bounds ( select_which, id, options )
# # 
# # Look for bounds which have the specified bound as a source (or all bounds from the specified
# # timescale). Convert these so that this relationship is broken. If $select_which is 'timescale',
# # then we are preparing to delete all the bounds in the specified timescale(s).

# sub detach_related_bounds {

#     my ($edt, $select_which, $id_list) = @_;
    
#     my $dbh = $edt->dbh;
    
#     # Construct the proper filter expression.
    
#     my ($filter, $extra);
#     my @sql;
    
#     if ( $select_which eq 'bound' )
#     {
# 	$filter = "source.bound_no in ($id_list)";
#     }
    
#     elsif ( $select_which eq 'timescale' )
#     {
# 	$filter = "source.timescale_no in ($id_list)";
#     }
    
#     elsif ( $select_which eq 'unupdated' )
#     {
# 	$filter = "source.timescale_no in ($id_list) and source.is_updated = 0";
#     }
    
#     else
#     {
# 	croak "bad value for 'select_which'\n";
#     }
    
#     # Detach all derived-color relationships.
    
#     push @sql,"	UPDATE $TIMESCALE_BOUNDS as tsb
# 		join $TIMESCALE_BOUNDS as source on tsb.color_no = source.bound_no
# 		SET tsb.color_no = 0
# 		WHERE $filter";
    
#     # Then detach all reference relationships.
    
#     push @sql,"	UPDATE $TIMESCALE_BOUNDS as tsb
# 		join $TIMESCALE_BOUNDS as source on tsb.refsource_no = source.bound_no
# 		SET tsb.refsource_no = 0
# 		WHERE $filter";
    
#     # Then detach all range relationships.
    
#     push @sql,"	UPDATE $TIMESCALE_BOUNDS as tsb
# 		join $TIMESCALE_BOUNDS as source on tsb.range_no = source.bound_no
# 		SET tsb.bound_type = if(tsb.bound_type = 'percent', 'absolute', tsb.bound_type),
# 		    tsb.range_no = 0,
# 		    tsb.base_no = if(tsb.bound_type = 'percent', 0, tsb.base_no)
# 		WHERE $filter";
    
#     # Then detach all base relationships.
    
#     push @sql," UPDATE $TIMESCALE_BOUNDS as tsb
# 		join $TIMESCALE_BOUNDS as source on tsb.base_no = source.bound_no
# 		SET tsb.bound_type = if(tsb.bound_type not in ('absolute','spike'), 'absolute', tsb.bound_type),
# 		    tsb.base_no = 0
# 		WHERE $filter";
    
#     # Now execute all of these.
    
#     try {

# 	foreach my $stmt ( @sql )
# 	{
# 	    print STDERR "$stmt\n\n" if $edt->debug;
	    
# 	    my $result = $dbh->do($stmt);
	    
# 	    if ( $edt->debug && $result && $result > 0 )
# 	    {
# 		print STDERR "    detached $result bounds\n\n";
# 	    }
# 	}
#     }
    
#     catch {
# 	print STDERR "ERROR: $_\n";
# 	$edt->add_condition("E_INTERNAL: an error occurred while deleting a record");
# 	return undef;
#     };
    
#     return 1;
# }


# sub check_timescale_attrs {
    
#     my ($edt, $op, $record_type, $attrs) = @_;
    
#     my @field_list;
#     my @value_list;
    
#     my $dbh = $edt->dbh;
    
#     # If any of the age values are being set, then compute and add the new precision as well.
    
#     foreach my $k ( qw(age offset age_error offset_error) )
#     {
# 	if ( defined $attrs->{$k} )
# 	{
# 	    my $prec = $attrs->{$k} =~ qr{ [.] (\d+) }xs ? length($1) : 0;
# 	    $attrs->{"${k}_prec"} = $prec;
# 	}
#     }
    
#     # Check the attributes one by one, according to their specified types.
    
#     my $specification = $record_type eq 'bound' ? \%TIMESCALE_BOUND_ATTRS : 
# 	\%TIMESCALE_ATTRS;
    
#     foreach my $k ( keys %$attrs )
#     {
# 	my $value = $attrs->{$k};
# 	my $type = $specification->{$k};
# 	my $quoted;
	
# 	# First make sure the field name and value are okay.
	
# 	if ( $k eq 'record_label' || ( $type && $type eq 'IGNORE' ) )
# 	{
# 	    next;
# 	}
	
# 	elsif ( $k =~ /_prec$/ )
# 	{
# 	    $quoted = $dbh->quote($value);
# 	}
	
# 	elsif ( ! defined $type )
# 	{
# 	    $edt->add_condition("W_BAD_FIELD: $k: unknown attribute");
# 	    next;
# 	}
	
# 	elsif ( ! defined $value )
# 	{
# 	    $edt->add_condition("W_BAD_VALUE: $k: not defined");
# 	    next;
# 	}
	
# 	# Special case the interval names
	
# 	elsif ( $k eq 'interval_name' || $k eq 'lower_name' )
# 	{
# 	    my $quoted_name = $dbh->quote($value);
	    
# 	    my ($id) = $dbh->selectrow_array("
# 		SELECT interval_no FROM $TIMESCALE_INTS WHERE interval_name like $quoted_name");
	    
# 	    if ( $id )
# 	    {
# 		$k =~ s/name/no/;
# 		$quoted = $id;
# 	    }
	    
# 	    elsif ( $edt->{condition}{CREATE_INTERVALS} )
# 	    {
# 		$dbh->do("INSERT INTO $TIMESCALE_INTS (interval_name) VALUES ($quoted_name)");
# 		$quoted = $dbh->last_insert_id(undef, undef, undef, undef);
		
# 		unless ( $quoted )
# 		{
# 		    croak "could not create interval $quoted_name\n";
# 		}
# 	    }
	    
# 	    else
# 	    {
# 		$edt->add_condition("C_CREATE_INTERVALS: $k: not found");
# 	    }
# 	}
	
# 	# Otherwise, check other types
	
# 	elsif ( $type eq 'varchar80' )
# 	{
# 	    if ( length($value) > 80 )
# 	    {
# 		$edt->add_condition("E_TOO_LONG: $k: must be 80 characters or less");
# 		next;
# 	    }
	    
# 	    $quoted = $dbh->quote($value);
# 	}
	
# 	elsif ( $type eq 'colorhex' )
# 	{
# 	    unless ( $value =~ qr{ ^ \# [0-9a-z]{6} $ }xsi )
# 	    {
# 		$edt->add_condition("E_BAD_COLOR: $k: must be a valid color in hexadecimal notation");
# 		next;
# 	    }
	    
# 	    $quoted = $dbh->quote(uc $value);
# 	}
	
# 	elsif ( $type eq 'pos_decimal' )
# 	{
# 	    unless ( $value =~ qr{ ^ (?: \d* [.] \d+ | \d+ [.] \d* | \d+ ) $ }xsi )
# 	    {
# 		$edt->add_condition("E_BAD_NUMBER: $k: must be a positive decimal number");
# 		next;
# 	    }
	    
# 	    $quoted = $value
# 	}
	
# 	elsif ( $type =~ /_no$/ )
# 	{
# 	    my ($idtype, $table, $label) = @{$TIMESCALE_REFDEF{$type}};
	    
# 	    if ( ref $value eq 'PBDB::ExtIdent' )
# 	    {
# 		$quoted = "$value";
# 	    }
	    
# 	    elsif ( $value =~ $ExternalIdent::IDRE{$idtype} && $2 > 0 )
# 	    {
# 		$quoted = $2;
# 	    }
	    
# 	    else
# 	    {
# 		$edt->add_condition("E_BAD_KEY: $k: must be a valid $label identifier");
# 		next;
# 	    }
	    
# 	    my $check_value;
	    
# 	    eval {
# 		($check_value) = $dbh->selectrow_array("SELECT $type FROM $table WHERE $type = $quoted");
# 	    };
	    
# 	    unless ( $check_value )
# 	    {
# 		$edt->add_condition("E_KEY_NOT_FOUND: $k: the identifier $quoted was not found in the database");
# 		next;
# 	    }
# 	}
	
# 	elsif ( ref $type eq 'HASH' )
# 	{
# 	    if ( $type->{lc $value} )
# 	    {
# 		$quoted = $dbh->quote(lc $value);
# 	    }
	    
# 	    else
# 	    {
# 		$edt->add_condition("E_BAD_VALUE: $k: value not acceptable");
# 		next;
# 	    }
# 	}
	
# 	else
# 	{
# 	    croak "check_attrs: bad data type for '$k'\n";
# 	}
	
# 	# Then create the proper SQL expressions for it.
	
# 	$k =~ s/_id$/_no/;
	
# 	# if ( $k eq 'age' )
# 	# {
# 	#     $k = "derived_age";
# 	# }
	
# 	push @field_list, $k;
# 	push @value_list, $quoted;
#     }
    
#     # Add the 'authorizer_no', 'enterer_no', 'modifier_no' values.
    
#     if ( $op eq 'add' )
#     {
# 	push @field_list, 'authorizer_no', 'enterer_no';
# 	push @value_list, $edt->{auth_info}{authorizer_no}, $edt->{auth_info}{enterer_no};
#     }
    
#     elsif ( ! $edt->{is_fixup} )
#     {
# 	push @field_list, 'modifier_no';
# 	push @value_list, $edt->{enterer_no};
#     }
    
#     return \@field_list, \@value_list;
# }


# sub check_timescale_values {
    
#     my ($edt, $new, $current) = @_;
    
#     if ( $current )
#     {
# 	if ( defined $new->{timescale_name} && $new->{timescale_name} eq '' )
# 	{
# 	    $edt->add_condition("E_PARAM: the value of 'timescale_name' must not be empty");
# 	}
	
# 	if ( defined $new->{timescale_type} && $new->{timescale_type} eq '' )
# 	{
# 	    $edt->add_condition("E_PARAM: the value of 'timescale_type' must not be empty");
# 	}
#     }
    
#     else
#     {
# 	unless ( defined $new->{timescale_name} && $new->{timescale_name} ne '' )
# 	{
# 	    $edt->add_condition("E_PARAM: the value of 'timescale_name' must not be empty");
# 	}
	
# 	unless ( defined $new->{timescale_type} && $new->{timescale_type} ne '' )
# 	{
# 	    $edt->add_condition("E_PARAM: the value of 'timescale_type' must not be empty");
# 	}
#     }
# }


# sub check_bound_values {

#     my ($edt, $new, $current) = @_;
    
#     my $new_age = $new->{age};
#     my $new_offset = $new->{offset};
#     my $new_bound_type = $new->{bound_type};
    
#     # If we are specifying any of 'bound_type', 'age', 'offset', then make sure 'age'
#     # and 'offset' have the proper range for the bound type.
    
#     if ( (defined $new->{age} && $new->{age} ne '') ||
# 	 (defined $new->{offset} && $new->{offset} ne '') ||
# 	 $new->{bound_type} )
#     {
# 	if ( $current )
# 	{
# 	    unless ( defined $new_age and $new_age ne '')
# 	    {
# 		$new_age = $current->{age};
# 	    }
	    
# 	    unless ( defined $new_offset and $new_offset ne '')
# 	    {
# 		$new_offset = $current->{offset};
# 	    }
	    
# 	    $new_bound_type ||= $current->{bound_type};
# 	}
	
# 	if ( $new_bound_type eq 'percent' )
# 	{
# 	    unless ( defined $new_offset && $new_offset ne '' && $new_offset >= 0.0 && $new_offset <= 100.0 )
# 	    {
# 		$edt->add_condition("E_OFFSET_RANGE: the value of 'offset' must be a percentage between 0 and 100.0 for this bound type");
# 	    }
# 	}
	
# 	elsif ( $new_bound_type eq 'offset' )
# 	{
# 	    unless ( defined $new_offset && $new_offset ne '' && $new_offset >= 0.0 && $new_offset <= 1000.0 )
# 	    {
# 		$edt->add_condition("E_OFFSET_RANGE: the value of 'offset' must be a value between 0 and 1000.0 Ma for this bound type");
# 	    }
# 	}
	
# 	elsif ( $new_bound_type eq 'absolute' || $new_bound_type eq 'spike' )
# 	{
# 	    unless ( defined $new_age && $new_age ne '' && $new_age >= 0.0 && $new_age <= 4600.0 )
# 	    {
# 		$edt->add_condition("E_AGE_RANGE: the value of 'age' must be a value between 0 and 4600.0 Ma for this bound type");
# 	    }
# 	}
#     }
        
#     return $edt;
# }


# # complete_bound_updates ( dbh )
# # 
# # Propagate any updates that have been made to the set of timescale boundaries. All bounds that
# # depend on the updated bounds will be recomputed, and all timescales containing them will be
# # updated as well.

# sub complete_bound_updates {
    
#     my ($edt) = @_;
    
#     my $dbh = $edt->dbh;
    
#     my ($result, $step);
    
#     # Catch any exceptions.
    
#     try {
	
# 	if ( $edt->debug )
# 	{
# 	    my $updated = $dbh->selectcol_arrayref("SELECT bound_no FROM $TIMESCALE_BOUNDS
# 							WHERE is_updated");

# 	    my $str = 'no bounds changed';

# 	    if ( ref $updated eq 'ARRAY' && @$updated )
# 	    {
# 		$str = join(',', @$updated);
# 	    }
	    
# 	    print STDERR "	propagating bound updates ($str)\n\n";
# 	}
	
# 	# First propagate changes to dependent boundaries.
	
# 	$step = 'propagating boundary changes';
	
# 	$result = $dbh->do("CALL complete_bound_updates");
	
# 	# Then check all boundaries in every timescale that had at least one updated boundary. Set
# 	# or clear the is_error flags on those boundaries and their timescales. Also update the
# 	# min_age and max_age values for those timescales.
	
# 	$step = 'checking boundaries for correctness';
	
# 	$result = $dbh->do("CALL check_updated_bounds");
	
# 	# Finally, clear all of the is_updated flags.
	
# 	$step = 'clearing the updated flags';
	
# 	$result = $dbh->do("CALL unmark_updated");
#     }
    
#     catch {

# 	print STDERR "ERROR: $_\n";
# 	$edt->add_condition("E_INTERNAL: an error occurred while $step");
	
#     };
# }


# sub bounds_updated {
    
#     my ($edt) = @_;
    
#     return ref $edt->{bound_updated} eq 'HASH' ? keys %{$edt->{bound_updated}} : ();
# }


# sub timescales_updated {
    
#     my ($edt) = @_;
    
#     return ref $edt->{timescale_updated} eq 'HASH' ? keys %{$edt->{timescale_updated}}: ();
# }


# propagate_boundary_changes ( dbh, source_bounds )
# 
# Propagate any changes to interval boundaries and timescales to the boundaries that refer to
# them. The flag is_updated indicates which ones have changed. If $update_all is specified, then
# update all bounds.

# sub propagate_boundary_changes {

#     my ($edt, $update_all) = @_;
    
#     # If errors have occurred, then the transaction will be rolled back, so there is no point in
#     # doing anything.
    
#     return if $edt->errors_occurred;
    
#     my $dbh = $edt->dbh;
    
#     my ($update_count, $update_previous, $loop_count);
#     my $sql;
    
#     # If we are directed to update all bounds, then set the is_update flag on all records.
    
#     if ( $update_all )
#     {
# 	$sql = "	UPDATE $TIMESCALE_BOUNDS as tsb SET is_updated = 1";
	
# 	$dbh->do($sql);
#     }
    
#     # Then execute a loop, propagating updated information one step at a time. Each newly updated
#     # record gets the update flag set as well. We stop the loop when either the number of updated
#     # records is zero or it is the same as the number the previous time through the loop,
#     # depending on the value of $UPDATE_MATCHED. We also have an absolute loop count to prevent
#     # runaway update loops.
    
#     # First update bounds
    
#     # $$$ need to fix: 1) percent computation is incorrect, 2) is_error flag setting works on
#     # incorrect timescale. 1 = possibly array count rather than element?
    
#     $update_count = 1;
#     $update_previous = 0;
#     $loop_count = 0;
    
#     while ( $update_count )
#     {
# 	$sql = "    UPDATE $TIMESCALE_BOUNDS as tsb
# 		join $TIMESCALE_BOUNDS as base on base.bound_no = tsb.base_no
# 		left join $TIMESCALE_BOUNDS as top on top.bound_no = tsb.range_no
# 	    SET tsb.is_updated = 1, tsb.updated = now(),
# 		tsb.derived_age = case tsb.bound_type
# 			when 'same' then base.derived_age
# 			when 'offset' then base.derived_age - tsb.offset
# 			when 'percent' then base.derived_age - (tsb.offset / 100) * ( base.derived_age - top.derived_age )
# 			end,
# 		tsb.derived_age_error = case tsb.bound_type
# 			when 'same' then base.age_error
# 			when 'offset' then base.age_error + tsb.offset_error
# 			when 'percent' then base.age_error + (tsb.offset_error / 100) * ( base.derived_age - top.derived_age )
# 			end
# 	    WHERE base.is_updated or top.is_updated or tsb.is_updated";
	
# 	print STDERR "$sql\n\n" if $edt->debug;
	
# 	$update_count = $dbh->do($sql);
	
# 	print STDERR "updated $update_count rows\n\n" if $edt->debug && $update_count;
	
# 	last if $EditTransaction::UPDATE_MATCHED && $update_count == $update_previous;
	
# 	$update_previous = $update_count;
	
# 	if ( $loop_count++ >= $UPDATE_LIMIT )
# 	{
# 	    $edt->add_condition("W_BAD_LOOP: iteration limit exceeded");
# 	    last;
# 	}
#     }
    
#     # Now do the same for colors
    
#     $update_count = 0;
    
#     while ( $update_count && $update_count > 0 )
#     {
# 	$sql = "
# 	    UPDATE $TIMESCALE_BOUNDS as tsb
# 		join $TIMESCALE_BOUNDS as base on base.bound_no = tsb.color_no
# 	    SET tsb.is_updated = 1, updated = now(),
# 		tsb.derived_color = base.color
# 	    WHERE base.is_updated or tsb.is_updated";
	
# 	print STDERR "$sql\n\n" if $edt->debug;
	
# 	$dbh->do($sql);
	
# 	($update_count) = $dbh->selectrow_array("SELECT ROW_COUNT()");
	
# 	print STDERR "updated $update_count rows\n\n" if $edt->debug && $update_count && $update_count > 0;
#     }

#     # And then for reference_nos
    
#     $update_count = 0;
    
#     while ( $update_count && $update_count > 0 )
#     {
# 	$sql = "
# 	    UPDATE $TIMESCALE_BOUNDS as tsb
# 		join $TIMESCALE_BOUNDS as base on base.bound_no = tsb.refsource_no
# 	    SET tsb.is_updated = 1, updated = now(),
# 		tsb.derived_reference_no = base.reference_no
# 	    WHERE base.is_updated or tsb.is_updated";
	
# 	print STDERR "$sql\n\n" if $edt->debug;
	
# 	$dbh->do($sql);
	
# 	($update_count) = $dbh->selectrow_array("SELECT ROW_COUNT()");
	
# 	print STDERR "updated $update_count rows\n\n" 
# 	    if $edt->debug && $update_count && $update_count > 0;
#     }
    
#     # Now, in all updated records, set or clear the 'is_different' bit depending upon
#     # whether the derived attributes are different from the main ones.
    
#     $sql = "
# 	UPDATE $TIMESCALE_BOUNDS as tsb
# 	SET tsb.is_different = tsb.age <> tsb.derived_age or tsb.age_error <> tsb.derived_age_error
# 		or tsb.color <> tsb.derived_color or tsb.reference_no <> tsb.derived_reference_no
# 	WHERE tsb.is_updated and tsb.is_locked";
    
#     print STDERR "$sql\n\n" if $edt->debug;
    
#     $dbh->do($sql);
    
#     if ( $edt->debug )
#     {
# 	($update_count) = $dbh->selectrow_array("SELECT ROW_COUNT()");
	
# 	print STDERR "updated is_different on $update_count locked rows\n\n" 
# 	    if $update_count && $update_count > 0;
#     }
    
#     # In all unlocked rows, set the main attributes from the derived ones. We clear the
#     # is_different bit just in case a newly unlocked record was previously different.
    
#     $sql = "
# 	UPDATE $TIMESCALE_BOUNDS as tsb
# 	SET tsb.is_different = 0,
# 	    tsb.age = tsb.derived_age,
# 	    tsb.age_error = tsb.derived_age_error,
# 	    tsb.color = tsb.derived_color,
# 	    tsb.reference_no = tsb.derived_reference_no
# 	WHERE tsb.is_updated and not tsb.is_locked";
    
#     print STDERR "$sql\n\n" if $edt->debug;
    
#     $dbh->do($sql);
    
#     if ( $edt->debug )
#     {
# 	($update_count) = $dbh->selectrow_array("SELECT ROW_COUNT()");
	
# 	print STDERR "updated is_different on $update_count unlocked rows\n\n" 
# 	    if $update_count && $update_count > 0;
#     }
# }


# sub update_and_check_timescales {

#     my ($edt, $timescale_ids) = @_;
    
#     # If errors have occurred, then the transaction will be rolled back, so there is no point in
#     # doing anything.
    
#     return if $edt->errors_occurred;
    
#     my $dbh = $edt->dbh;
#     my $update_count;
#     my $sql;
    
#     # First, we need to figure out which timescales contain updated boundaries.
    
#     my ($timescale_list) = $dbh->do("
# 	SELECT group_concat(distinct timescale_no) FROM $TIMESCALE_BOUNDS
# 	WHERE is_updated");
    
#     # If we are given a list of timescale ids to update, join this in too.
    
#     if ( $timescale_ids && $timescale_list )
#     {
# 	$timescale_list = join(',', $timescale_ids, $timescale_list);
#     }
    
#     elsif ( $timescale_ids )
#     {
# 	$timescale_list = $timescale_ids;
#     }
    
#     # Return if the combined list is empty.
    
#     return unless $timescale_list;
    
#     # Update min and max ages for these timescales.
    
#     $sql = "  UPDATE $TIMESCALE_DATA as ts join 
# 		(SELECT timescale_no, max(age) as max_age, min(age) as min_age
# 		 FROM $TIMESCALE_BOUNDS as tsb
# 		 WHERE timescale_no in ($timescale_list) GROUP BY timescale_no) as n using (timescale_no)
# 	    SET ts.max_age = n.max_age, ts.min_age = n.min_age";
    
#     print STDERR "$sql\n\n" if $edt->debug;
    
#     $update_count = $dbh->do($sql);
    
#     # Now set or clear the is_error flags for every boundary in these timescales.
    
#     # We will be able to substantially simplify the following expression once we are able to
#     # upgrade to MariaDB 10.2, which introduces common table expressions. For now:
#     # 
#     # We select all of the bounds in the updated timescales twice, as b1 and b2. In each
#     # selection, we order them by timescale and age and number the rows using @r1 and @r2. We then
#     # join each row in b1 to the previous row from the same timescale in b2, so that we can check
#     # that the upper interval for each boundary matches the lower interval from the previous
#     # boundary, and that the age of each boundary is greater than the age of the previous
#     # boundary. This check is done in the second and third lines. Initial boundaries don't have a
#     # matching row in b2, so the lower_no and age will be null. The expressions 'bound_ok' and 
#     # 'age_ok' are then used to set the error flag for each boundary.
    
#     $sql = "SET \@r1=0, \@r2=0";
    
#     print STDERR "$sql\n\n" if $edt->debug;
    
#     $dbh->do($sql);
    
#     $sql = "  UPDATE $TIMESCALE_BOUNDS as tsb join
# 		(SELECT b1.bound_no, (b1.age > b2.age or b2.age is null) as age_ok,
# 		    (b1.interval_no = b2.lower_no or (b1.interval_no = 0 and b2.lower_no is null)) as bound_ok
# 		 FROM
# 		  (select (\@r1 := \@r1 + 1) as row, bound_no, timescale_no, age, interval_no from $TIMESCALE_BOUNDS 
# 		   WHERE timescale_no in ($timescale_list) ORDER BY timescale_no, age) as b1 LEFT JOIN
# 		  (select (\@r2 := \@r2 + 1) as row, timescale_no, age, lower_no FROM $TIMESCALE_BOUNDS
# 		   WHERE timescale_no in ($timescale_list) ORDER BY timescale_no, age) as b2 on
# 			b1.row = b2.row + 1 and b1.timescale_no = b2.timescale_no) as bound_check using (bound_no)
# 	      SET is_error = not(bound_ok and age_ok)";
    
#     print STDERR "$sql\n\n" if $edt->debug;
    
#     $update_count = $dbh->do($sql);
    
#     # Finally, we need to update the error flag for each of the updated timescales.
    
#     $sql = "  UPDATE $TIMESCALE_DATA as ts join 
# 		(SELECT timescale_no, max(is_error) as is_error
# 		 FROM $TIMESCALE_BOUNDS WHERE timescale_no in ($timescale_list)
# 		 GROUP BY timescale_no) as any_tsb
# 	      SET ts.is_error = any_tsb.is_error";
    
#     print STDERR "$sql\n\n" if $edt->debug;
    
#     $update_count = $dbh->do($sql);
# }


# sub clear_update_flags {
    
#     my ($edt) = @_;
    
#     # If errors have occurred, then the transaction will be rolled back, so there is no point in
#     # doing anything.
    
#     return if $edt->errors_occurred;
    
#     # Otherwise, clear all update flags.
    
#     my $dbh = $edt->dbh;
    
#     my $sql = " UPDATE $TIMESCALE_BOUNDS SET is_updated = 0";
    
#     $dbh->do($sql);
# }


1;
