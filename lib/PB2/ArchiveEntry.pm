#  
# ArchiveEntry
# 
# A role that allows entry and manipulation of records representing data archives. This is
# a template for general record editing, in situations where the records in a table are
# independent of the rest of the database.
# 
# Author: Michael McClennen

use strict;

use lib '..';

package PB2::ArchiveEntry;

use TableDefs qw(%TABLE);

use ExternalIdent qw(generate_identifier VALID_IDENTIFIER);
use TableData qw(complete_ruleset);

use HTTP::Validate qw(:validators);
use Carp qw(carp croak);
use MIME::Base64;

use Moo::Role;


our (@REQUIRES_ROLE) = qw(PB2::Authentication PB2::CommonData PB2::CommonEntry PB2::ArchiveData);


# initialize ( )
# 
# This routine is called by the Web::DataService module, and allows us to define
# the elements necessary to handle the operations implemented by this class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # Value sets for specifying data entry options.
    
    $ds->define_set('1.2:archives:allowances' =>
	{ value => 'CREATE' },
	    "Allows new records to be created. Without this parameter,",
	    "this operation will only update existing records. This is",
	    "included as a failsafe to make sure that new records are not",
	    "added accidentally due to bad interface code.",
	{ value => 'PROCEED' },
	    "If some but not all of the submitted records contain errors",
	    "then allowing C<B<PROCEED>> will cause any records that can",
	    "be added or updated to proceed. Otherwise, the entire operation",
	    "will be aborted if any errors occur.",
	{ value => 'ARCHIVE_HAS_DOI' },
	    "Allows an archive with a DOI to be changed. This allows the",
	    "user interface to get a confirmation from the user and then",
	    "repeat the operation with this allowance.");
    
    # Rulesets for entering and updating data.
    
    $ds->define_ruleset('1.2:archives:entry' =>
	{ param => '_label', valid => ANY_VALUE },
	    "This parameter is only necessary in body records, and then only if",
	    "more than one record is included in a given request. This allows",
	    "you to associate any returned error messages with the records that",
	    "generated them. You may provide any non-empty value.",
	{ optional => '_operation', valid => ANY_VALUE },
	    "You may provide a value for this attribute in any record",
	    "submitted to this data service. This specifies the operation.",
	    "to be performed using this record, overriding the automatic",
	    "determination. The main use of this field is with the value 'delete',",
	    "which causes this record to be deleted from the database.",
	{ param => 'archive_id', valid => VALID_IDENTIFIER('DAR'), alias => ['archive_no' ,'oid', 'id' ] },
	    "The identifier of the data archive record to be updated. If it is",
	    "empty, a new record will be created. You can also use the alias B<C<archive_no>>.");
    
    $ds->define_ruleset('1.2:archives:addupdate' =>
	{ allow => '1.2:archives:specifier' },
	{ optional => 'SPECIAL(show)', valid => '1.2:archives:optional_output' },
	{ optional => 'allow', valid => '1.2:archives:allowances', list => ',' },
	    "Allow the operation to proceed with certain conditions or properties.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    $ds->define_ruleset('1.2:archives:addupdate_body' =>
	">>The body of this request must be either a single JSON object, or an array of",
	"JSON objects, or else a single record in C<application/x-www-form-urlencoded> format.",
	"The fields in each record must be as specified below. If no specific documentation is given",
	"the value must match the corresponding column in the C<B<$TABLE{ARCHIVES}>> table",
	"in the database.",
	{ allow => '1.2:archives:entry' });
    
    $ds->define_ruleset('1.2:archives:delete' =>
	">>The following parameter may be given either in the URL or in",
	"the request body. Either way, you may specify more than one value,",
	"as a comma-separated list.",
	{ param => 'archive_id', valid => VALID_IDENTIFIER('DAR'), list => ',',
	  alias => ['archive_no', 'id', 'oid'] },
	    "The identifier(s) of the data archive record(s) to delete.",
	{ optional => 'allow', valid => '1.2:archives:allowances', list => ',' },
	    "Allow the operation to proceed with certain conditions or properties.",
	{ allow => '1.2:special_params' },
	"^You can also use any of the L<special parameters|node:special>  with this request");
    
    my $dbh = $ds->get_connection;
    
    complete_ruleset($ds, $dbh, '1.2:archives:entry', 'ARCHIVES');
}


sub update_archives {
    
    my ($request, $arg) = @_;
    
    my $dbh = $request->get_connection;
    
    # First get the parameters from the URL, and/or from the body if it is from a web form. In the
    # latter case, it will necessarily specify a single record only.
    
    my $allowances = { };
    
    my $main_params = $request->get_main_params($allowances, '1.2:archives:specifier');
    my $perms = $request->require_authentication('ARCHIVES');
    
    # Then decode the body, and extract input records from it. If an error occured, return an
    # HTTP 400 result. For now, we will look for the global parameters under the key 'all'.
    
    my (@records) = $request->unpack_input_records($main_params, '1.2:archives:addupdate_body');
    
    if ( $request->errors )
    {
	die $request->exception(400, "Bad data");
    }
    
    # If we get here without any errors being detected so far, create a new EditTransaction object to
    # handle this operation.
    
    my $edt = EditTransaction->new($request, $perms, 'ARCHIVES', $allowances);
    
    # Now go through the records and handle each one in turn. This will check every record and
    # queue them up for insertion and/or updating.
    
    foreach my $r (@records)
    {
	$edt->process_record('ARCHIVES', $r);
    }
    
    # If no errors have been detected so far, execute the queued actions inside a database
    # transaction. If any errors occur during that process, the transaction will be automatically
    # rolled back. Otherwise, it will be automatically committed.
    
    $edt->commit;
    
    # Now handle any errors or warnings that may have been generated.
    
    $request->collect_edt_warnings($edt);
    
    if ( $edt->errors )
    {
    	$request->collect_edt_errors($edt);
    	die $request->exception(400, "Bad request");
    }
    
    # If the output format is 'larkin', return just a single success record.
    
    if ( $request->output_format eq 'larkin' )
    {
	if ( $edt->inserted_keys )
	{
	    return $request->data_result('{"Success":"Successfully added data archive"}' . "\n");
	}

	else
	{
	    return $request->data_result('{"Success":"Successfully updated data archive"}' . "\n");
	}
    }
    
    # Otherwise, return all affected records.
    
    my @deleted_keys = $edt->deleted_keys;
    
    $request->list_deleted_archives(\@deleted_keys, $edt->key_labels) if @deleted_keys;
    
    my @existing_keys = ($edt->inserted_keys, $edt->updated_keys, $edt->replaced_keys);
    
    $request->list_updated_archives($dbh, \@existing_keys, $edt->key_labels) if @existing_keys;
}


sub list_updated_archives {
    
    my ($request, $dbh, $keys_ref, $labels_ref) = @_;
    
    my $key_list = join(',', @$keys_ref);
    
    $request->{my_record_label} = $labels_ref;
    
    $request->extid_check;
    
    # If a query limit has been specified, modify the query accordingly.
    
    my $limit = $request->sql_limit_clause(1);
    
    # If we were asked to count rows, modify the query accordingly
    
    my $calc = $request->sql_count_clause;
    
    # Determine the necessary joins.
    
    # my ($join_list) = $request->generate_join_list('tsb', $tables);
    
    # Generate the main query.
    
    $request->{main_sql} = "
	SELECT $calc arch.* FROM $TABLE{ARCHIVES} as arch
	WHERE arch.archive_no in ($key_list)
	ORDER BY arch.archive_no";
    
    print STDERR "$request->{main_sql}\n\n" if $request->debug;
    
    my $results = $dbh->selectall_arrayref($request->{main_sql}, { Slice => { } });
    
    # If we were asked to get the count, then do so
    
    $request->sql_count_rows;
    
    # If we got some results, return them.
    
    $request->add_result($results);
}


sub delete_archives {

    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    
    # Get the archive identifiers to delete from the URL paramters. This operation takes no body.
    
    my (@id_list) = $request->clean_param_list('archive_id');
    
    # Check for any allowances.
    
    my $allowances = { MULTI_DELETE => 1 };
    
    if ( my @allowance = $request->clean_param_list('allow') )
    {
	$allowances->{$_} = 1 foreach @allowance;
    }
    
    # Determine our authentication info, and then create an EditTransaction object.
    
    my $perms = $request->require_authentication('ARCHIVES');
    
    my $edt = EditTransaction->new($request, $perms, 'ARCHIVES', $allowances);
    
    # Then go through the records and handle each one in turn.
    
    foreach my $id (@id_list)
    {
	$edt->delete_record('ARCHIVES', $id);
    }
    
    # After the deletion(s) are done, adjust the auto_increment value of the table to one more
    # than the new maximum archive_no, whatever that may be.
    
    $edt->do_sql("ALTER TABLE $TABLE{ARCHIVES} auto_increment=1");
    
    # If no errors have been detected so far, execute the queued actions inside a database
    # transaction. If any errors occur during that process, the transaction will be automatically
    # rolled back. Otherwise, it will be automatically committed.
    
    $edt->execute;
    
    # Now handle any errors or warnings that may have been generated.
    
    $request->collect_edt_warnings($edt);
    
    if ( $edt->errors )
    {
    	$request->collect_edt_errors($edt);
    	die $request->exception(400, "Bad request");
    }
    
    # Then return one result record for each deleted database record.
    
    $request->extid_check;
    
    my @deleted_keys = $edt->deleted_keys;
    
    $request->list_deleted_archives(\@deleted_keys, $edt->key_labels) if @deleted_keys;
}


sub list_deleted_archives {

    my ($request, $keys_ref, $labels_ref) = @_;
    
    foreach my $key ( @$keys_ref )
    {
	my $archive_id = $request->{block_hash}{extids} ? generate_identifier('DAR', $key) : $key;
	my $record = { archive_no => $archive_id, status => 'deleted' };
	$request->add_result($record);
    }
}


# check_archive ( )
#
# This method is called as a before_execute_hook. Its job is to check for the presence of an
# 'archive_name' parameter. If this parameter is present and has a non-empty value, then the
# process of creating or updating a data archive is initiated.

sub check_archive {
    
    my ($request) = @_;
    
    my ($dbh, $sql);
    
    # If this request has a 'clean_param' method and the parameter 'archive_title' has a non-empty
    # value, then we need to check if it is possible to create a data archive to hold the results
    # of this request.
    
    return unless $request->can('clean_param');
    
    if ( my $title = $request->clean_param('archive_title') )
    {
	my $perms = $request->PB2::Authentication::require_authentication('ARCHIVES');
	my $enterer_no = $perms->{enterer_no};
	
	# Creating a data archive is only allowed for logged-in users that have an enterer number.
	
	die $request->exception(401, "You must be a database contributor in order to create an archive")
	    unless $enterer_no;
	
	# Archives created by a particular enterer must have unique titles. If an existing archive
	# is found with the specified title, return a 400 error unless the parameter
	# 'archive_replace' was specified with a true value. Archives that have a DOI assigned to
	# them are considered immutable and their contents cannot be replaced.
	
	my $replace_existing = $request->clean_param('archive_replace');
	
	$dbh = $request->get_connection;
	
	my $quoted_title = $dbh->quote($title);
	
	$sql = "SELECT archive_no, doi FROM $TABLE{ARCHIVES}
		WHERE title = $quoted_title and enterer_no = $enterer_no";
	
	$request->debug_line("$sql\n\n");
	
	my ($archive_no, $doi) = $dbh->selectrow_array($sql);
	
	if ( $archive_no )
	{
	    if ( $doi )
	    {
		$request->add_error("E_IMMUTABLE: archive dar:$archive_no is owned by you and has the same title.");
		$request->add_error("This archive has a DOI, and so its contents are immutable.");
		die $request->exception(400);
	    }
	    
	    elsif ( ! $replace_existing )
	    {
		$request->add_error("E_EXISTING: archive dar:$archive_no is owned by you and has the same title.");
		$request->add_error("To replace its contents, retry this request with the parameter 'archive_replace=yes'.");
		die $request->exception(400);
	    }
	    
	    else
	    {
		$request->{my_archive_no} = $archive_no;
	    }
	    
	}
	
	# Otherwise, a new archive record will be created. Set a 'before_output_hook' for this
	# request, so that we can create the record and open the archive file after the operation
	# method has completed successfully and before the output is written.
	
	$request->{hook_enabled}{before_output_hook} = \&open_archive;
	$request->{my_perms} = $perms;
    }
}


# open_archive ( )
#
# This method is called as a before_output_hook. At this point, we know that the request
# parameters were good, the operation succeeded, and we have some data to archive.

sub open_archive {

    my ($request) = @_;

    my $ds = $request->ds;
    my $dbh = $request->get_connection;
    my $archive_no = $request->{my_archive_no};
    my $perms = $request->{my_perms};
    
    my $result;
    
    # Make sure that we have the proper credentials to proceed.
    
    die $request->exception(500, "Permissions not found") unless $perms && $perms->{enterer_no};
    
    # Create an EditTransaction for inserting or updating a data archive record. The CREATE
    # allowance lets us create a new record if we don't already have an archive_no for this
    # request.
    
    my $edt = EditTransaction->new($request, $perms, 'ARCHIVES', { CREATE => 1,
								   IMMEDIATE_EXECUTION => 1 });
    
    # If we are replacing the contents of an existing archive record, the user can also update the
    # 'authors' and 'description' fields. A new record can have both of those fields and must also
    # have an archive title.
    
    my $title = $request->clean_param('archive_title');
    my $authors = $request->clean_param('archive_authors');
    my $desc = $request->clean_param('archive_desc');
    
    my $record = { };
    
    $record->{archive_no} = $archive_no;
    $record->{title} = $title;
    $record->{authors} = $authors if $authors;
    $record->{description} = $desc if $desc;
    $record->{fetched} = 'now';
    
    my ($uri_path, $uri_args) = split /[?]/, $request->request_url, 2;
    
    $record->{uri_path} = $uri_path;
    
    $uri_args =~ s/&archive_title=[^&]*//;
    $uri_args =~ s/&archive_authors=[^&]*//;
    $uri_args =~ s/&archive_desc=[^&]*//;
    $uri_args =~ s/&archive_replace=[^&]*//;
    # $uri_args =~ s/&_=[^&]*//;
    
    $record->{uri_args} = $uri_args;
    
    $request->datainfo_url("$uri_path?$uri_args");
    
    if ( $archive_no )
    {
	$edt->update_record('ARCHIVES', $record);
    }
    
    else
    {
	$result = $edt->insert_record('ARCHIVES', $record);
	$archive_no = $request->{my_archive_no} = $result;
    }
    
    # We are going to ignore warnings, but we need to abort if an error occurs.
    
    if ( $edt->errors )
    {
	$request->PB2::CommonEntry::collect_edt_errors($edt);
	die $request->exception(400, "Bad request");
    }
    
    # Open a file in which to store the data. This is actually a pipe to the 'gzip' command. If
    # the open succeeds, then commit the database transaction. We explicitly test that the file is
    # writable (or that the directory is writable if the file doesn't exist) because the open will
    # not generally fail in that case.
    
    my $outdir = "/var/paleomacro/archives";
    my $outfile = "/var/paleomacro/archives/$archive_no.gz";
    my $outfh;
    
    my $is_writable = -w $outfile || (-w $outdir && ! -e $outfile);
    
    if ( $is_writable && open($outfh, '|-', "gzip -9 > $outfile") )
    {
	$ds->output_to_file($request, $outfh, \&archive_response);
        $edt->commit;
	
	# Make one more check in case an error occurs on commit (it shouldn't).
	
	if ( $edt->errors )
	{
	    close $outfh;
	    $request->PB2::CommonEntry::collect_edt_errors($edt);
	    die $request->exception(500, "An error occurred on commit");
	}
    }
    
    # If the open fails, roll back the database transaction and throw an exception.
    
    else
    {
	$edt->rollback;
	die $request->exception(500, "Archive error: could not open pipe to 'gzip -9 > $outfile: $!");
    }

    # Otherwise, we are good to proceed. Remove all archive parameters from the request, so that
    # they won't be reported in the parameter list, and add the same values as extra datainfo
    # parameters.
    
    delete $request->{clean_params}{archive_no};
    delete $request->{clean_params}{archive_id};
    delete $request->{clean_params}{archive_title};
    delete $request->{clean_params}{archive_authors};
    delete $request->{clean_params}{archive_desc};
    delete $request->{clean_params}{archive_replace};
    
    $request->set_extra_datainfo('archive_no', 'Archive Number', $archive_no);
    $request->set_extra_datainfo('archive_title', 'Archive Title', $title);
    $request->set_extra_datainfo('archive_authors', 'Archive Authors', $authors) if $authors;
    $request->set_extra_datainfo('archive_desc', 'Archive Description', $desc) if $desc;
}


# archive_response ( )
#
# This method is called to generate the content that will be returned to the client to indicate
# that archive creation/update succeeded.

sub archive_response {

    my ($request) = @_;
    
    my $archive_no = $request->{my_archive_no};
    my $format = $request->output_format;
    my $message = "Created archive dar:$archive_no";
    
    if ( $format eq 'json' )
    {
	return "\"$message\"\n";
    }
    
    else {
	return "$message\n";
    }
}


1;
