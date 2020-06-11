# -*- fill-column: 200 -*-
#

use Test::More tests => 1;

pass('placeholder');

# Things to test:
#
# 1. Table properties: [$$$]
#
# CAN_POST [edt-21-permissions.t 'basic', 'can_post']
# CAN_VIEW [$$$]
# CAN_MODIFY [edt-21-permissions.t 'can_modify']
# ALLOW_DELETE [edt-21-permissions.t 'allow_delete']
# ALLOW_INSERT_KEY [edt-21-permissions.t 'allow_insert_key']
# BY_AUTHORIZER [edt-21-permissions.t 'by_authorizer']
# PRIMARY_KEY [edt-01-basic.t 'create objects']
# PRIMARY_ATTR [edt-14-records.t 'primary_attr']
# SUPERIOR_TABLE [EditTest.pm, $$$]
# SUPERIOR_KEY [$$$]
# TABLE_COMMENT [EditTest.pm, line 39]

# 2. Column properties: [$$$]
# 
# ALTERNATE_NAME [edt-14-records.t 'alternate_name']
# FOREIGN_KEY [edt-22-validate.t 'foreign_table']
# FOREIGN_TABLE [edt-22-validate.t 'foreign_table']
# EXTID_TYPE [edt-22-validate.t 'foreign keys']
# ALLOW_TRUNCATE [edt-23-datatypes.t 'text', 'fixed']
# VALUE_SEPARATOR [edt-23-datatypes.t 'sets']
# VALIDATOR [edt-22-validate.t 'validators']
# REQUIRED [edt-22-validate.t 'required']
# ADMIN_SET [edt-17-admin.t 'admin_set']
# IGNORE [$$$]
# COLUMN_COMMENT [EditTest.pm, line 42]

# 3. Table permissions: [$$$]
# 
# admin - can insert, update, delete, etc.
# 		[edt-21-permissions.t 'can_post', 'can_modify', 'allow_delete', 'allow_insert_key']
# modify - can insert, update, and can delete if table allows
# 		[edt-21-permissions.t 'can_post', 'can_modify', 'allow_delete']
# post - can insert, and can update and delete own records [edt-21-permissions.t 'can_post', 'allow_delete']
# view - can view, but do nothing else [$$$]

# 4. Allowances: [$$$]
# 
# CREATE [edt-01-basic 'create objects']
# LOCKED [edt-17-admin.t 'admin_lock admin']
# MULTI_DELETE [edt-34-delete.t 'basic']
# ALTER_TRAIL [edt-16-modified.t 'crmod admin', 'authent admin']
# DEBUG_MODE [edt-01-basic.t 'debug output']
# SILENT_MODE [edt-01-basic.t 'debug output']
# IMMEDIATE_MODE [edt-10-transaction.pm 'immediate']
# PROCEED [edt-11-proceed.pm 'proceed_mode']
# NOT_FOUND [edt-11-proceed.pm 'not_found']
# NO_RECORDS [edt-11-proceed.pm 'no_records']
# NO_LOG_MODE [$$$]
# register_allowances [EditTest.pm, edt-01-basic.t 'allowances']
# extra allowance defined by EditTest.pm [edt-01-basic.t 'allowances']
# check that all of these are accepted, and no others. [edt-01-basic.t 'allowances']

# 5. EditTest->new [DONE]
# 
# create with dbh [edt-01-basic.t 'create objects']
# create with request [edt-01-basic.t 'create objects']
# bad allowance -> W_ALLOW [edt-01-basic.t 'allowances']

# 6. Accessor methods [DONE]
# 
# $edt->dbh [edt-01-basic.t 'accessors']
# $edt->request [edt-02-request.t 'request']
# $edt->transaction [edt-10-transaction.t 'execute', 'immediate', 'errors]
# $edt->has_started [edt-10-transaction.t 'execute', 'immediate', 'errors]
# $edt->has_finished [edt-10-transaction.t 'execute', 'immediate', 'errors]
# $edt->has_committed [edt-10-transaction.t 'execute', 'immediate', 'errors']
# $edt->is_active [edt-10-transaction.t 'execute', 'immediate', 'errors]
# $edt->can_proceed [edt-10-transaction.t 'execute', 'immediate', 'errors']
# $edt->perms [edt-01-basic.t 'accessors']
# $edt->role [edt-01-basic.t 'accessors']
# $edt->allows [edt-01-basic.t 'allowances']

# 7. Debugging [DONE]
#
#  - override and capture calls to debug_line [edt-01-basic.t 'debug output']
#  - debugging output can be turned on and off [edt-01-basic.t 'debug output']
#  - silent mode prevents exception messages from being printed out
# 	[edt-01-basic.t 'test exceptions']
#  - silent mode can be turned on and off [edt-01-basic.t 'debug output']

# 8. errors, warnings, and cautions [DONE]
# 
#  $edt->add_condition [edt-12-conditions.t 'basic']
#  - works before first action, works after actions [edt-12-conditions.t 'basic']
#  - works with specific actions [edt-12-conditions.t 'basic']
#  - works with both undef and 'main' as first argument [edt-12-conditions.t 'basic']
#  - throws exception if bad code is given [edt-12-conditions.t 'register']
#  - errors are demoted to warnings with PROCEED
#	[edt-11-proceed.t 'proceed_mode', edt-12-conditions.t 'proceed']
#  - E_NOT_FOUND is demoted to warning with NOT_FOUND
#	[edt-11-proceed.t 'not_found', edt-12-conditions.t 'notfound']
#
#  $edt->register_condition [edt-12-conditions.t 'register']
#  - throws exception if code does not match proper pattern [edt-12-conditions.t 'register']
#
#  $edt->errors [edt-12-conditions.t 'basic']
#  $edt->warnings [edt-12-conditions.t 'basic']
#  $edt->conditions [edt-12-conditions.t 'basic']
#  $edt->error_strings [edt-12-conditions.t 'basic']
#  $edt->warning_strings [edt-12-conditions.t 'basic']
#  $edt->generate_msg [edt-12-conditions.t 'generate_msg']
#
#  variable substitution in templates [edt-12-conditions.t 'templates']
#  selection of template by first parameter [edt-12-conditions.t 'templates']
#  
#  conditions work properly from the following overrideable methods:
#   - authorize_action [edt-20-subclass.t 'authorize']
#   - validate_action [edt-20-subclass.t 'validate']
#   - before_action [edt-20-subclass.t 'before and after']
#   - after_action [edt-20-subclass.t 'before and after']
#   - cleanup_action [edt-20-subclass.t 'before and after']
#   - initialize_transaction [edt-20-subclass.t 'initialize and finalize']
#   - finalize_transaction [edt-20-subclass.t 'initialize and finalize']
#   - cleanup_transaction [edt-20-subclass.t 'initialize and finalize']

# 10. record labels [$$$]
# 
#  - record label is carried through properly to conditions and messages [edt-12-condition.t 'basic']
#  - labels are properly generated for unlabeled records [edt-14-records.t 'basic']
#  - $edt->key_labels [edt-30-insert.t 'insert with labels'; $$$]
#  - labels can be referred to in subsequent insertions [edt-30-insert.t 'insert with labels']
#  - labels can be referred to in subsequent updates [$$$]

# 11. transaction control [DONE]
# 
# $edt->start_transaction [DONE]
#  - check that transaction status is properly changed [edt-10-transaction.t 'start']
#  - check that database operations are not carried out until execute or start_execution
#	is called [edt-10-transaction.t 'start']
#  - exception when called after committed transactions [edt-10-transaction.t 'execute']
#  - make sure that calling a second time on an active transaction is okay [edt-10-transaction.t 'start']
# 
# $edt->start_execution [DONE]
#  - check that transaction status is properly changed [edt-10-transaction.t 'start']
#  - check that pending database operations are immediately done [edt-10-transaction.t 'start']
#  - check that subsequent operations are immediately done [edt-10-transaction.t 'start', 'immediate']
#  - exception when called after transaction commits [edt-10-transaction.t 'execute']
#  - make sure that calling a second time on an active transaction is okay [edt-10-transaction.t 'start']
# 
# $edt->execute [DONE]
#  - pending operations are carried out [edt-10-transaction.t 'execute']
#  - transaction status is properly changed [edt-10-transaction.t 'execute']
#  - initialize_transaction and finalize_transaction are properly called
#	[edt-10-transaction.t 'execute']
# 
# $edt->commit [DONE]
#  - has the same effect as execute [edt-10-transaction.t 'execute']
#  - calling a second time is okay [edt-10-transaction.t 'execute']
#  - okay to call before a transaction starts, returns false [edt-10-transaction.t 'execute']
#  - returns true after commit, no matter how many times called [edt-10-transaction.t 'execute']
#  - calling after a transaction fails returns false [edt-10-transaction.t 'errors']
# 
# $edt->rollback [DONE]
#  - really does a database rollback [edt-10-transaction.t 'rollback']
#  - ok to call before a transaction starts [edt-10-transaction.t 'rollback']
#  - returns false when called before transaction starts [edt-10-transaction.t 'rollback']
#  - returns true when called after transaction starts [edt-10-transaction.t 'rollback']
# 
# authorize_action and validate_action [DONE]
#  - check status with $edt->transaction, $edt->active, and $edt->can_proceed
#	[edt-20-subclass.t 'authorize', 'validate']
#  - check both before and after $edt->start_execution [edt-10-transaction.t 'errors']
# 
# general [DONE]
# - automatic rollback when object goes out of scope [edt-01-basic.t 'out of scope']
# - automatic rollback when a new transaction is started before an old one is finished
#	[edt-15-interlock.t 'interlock']


# 12. insert_record [DONE]
# 
#  - can insert with CREATE [edt-10-transaction.t 'basic']
#  - check for C_CREATE without CREATE [edt-30-insert.t 'errors']
#  - can insert with 'post' permission [edt-21-permissions.t 'can_post']
#  - can insert with 'modify' permission [edt-21-permissions.t 'can_modify']
#  - can insert with 'admin' permission [edt-21-permissions.t 'basic']
#  - check for E_PERM without one of these permissions [edt-21-permissions.t 'basic']
#  - check for E_HAS_KEY if a key is given [edt-30-insert.t 'errors']
#  - check for authorize_action and validate_action (by overriding) [edt-20-subclass.t 'authorize', 'validate']
#  - in validate_action, check $action->get_keyexpr and $action->get_keylist [edt-30-insert.t 'subclass']
#  - check or an exception if a bad table name is given [edt-30-insert.t 'bad']
#  - an SQL statement deliberately created to crash results in E_EXECUTE
#	[edt-11-proceed.t 'proceed_mode', edt-20-subclass.t 'before and after']
#  - check for E_DUPLICATE if an insertion causes a duplicate key error [edt-30-insert.t 'execution errors']
#  - check that we can insert using label references [edt-30-insert.t 'insert with labels']

# 13. update_record [DONE]
#
#  - can update with a key [edt-31-update.t 'basic']
#  - check for E_NO_KEY without one [edt-31-update.t 'basic']
#  - check for E_NOT_FOUND if key does not exist in table [edt-31-update.t 'basic']
#  - can update own records by default but not others [edt-21-permissions.t 'basic', 'can_modify']
#  - can update with 'modify' permission [edt-21-permissions.t 'can_post']
#  - can update with 'admin' permission [edt-21-permissions.t 'basic']
#  - check for E_PERM without one of these permissions [edt-21-permissions.t 'basic']
#  - check for authorize_action and validate_action (by overriding) [edt-31-update.t 'subclass']
#  - in validate_action, check $action->get_keyexpr and $action->get_keylist [edt-31-update.t 'subclass']
#  - check for exception if a bad table name is given [edt-31-update.t 'bad']
#  - check for E_EXECUTE on an SQL statement deliberately created to fail [edt-31-update.t 'execution errors']
#  - check for E_DUPLICATE if an update causes a duplicate key error [edt-31-update.t 'execution errors']

# 14. replace_record [$$$]
# 
#  - can replace with a key [edt-33-replace.t 'basic']
#  - check for E_NO_KEY without one [edt-33-replace.t 'basic']
#  - check for E_NOT_FOUND if key does not exist in table and INSERT_KEY is not set [$$$]
#  - can replace with key if INSERT_KEY is set [$$$]
#  - can replace own records [edt-33-replace.t 'basic']
#  - can replace with 'modify' permission and key found [$$$]
#  - can replace with 'admin' permission and key found [$$$]
#  - check for E_PERM without one of these permissions [$$$]
#  - check for authorize_action and validate_action (by overriding) [edt-33-replace.t 'subclass']
#  - in validate_action, check $action->get_keyexpr and $action->get_keylist [edt-33-replace.t 'subclass']
#  - check for E_EXECUTE on an SQL statement deliberately created to crash [edt-33-replace.t 'execution errors']
#  - check for E_EXECUTE on an SQL statement deliberately created to fail [$$$]

# 15. delete_record [$$$]
#
#  - can delete with a key [edt-34-delete.t 'basic']
#  - check for E_NO_KEY without one [edt-34-delete.t 'errors']
#  - check for E_NOT_FOUND if key does not exist in table [edt-34-delete.t 'errors']
#  - can delete with 'delete' permission [$$$]
#  - can delete with 'admin' permission [$$$]
#  - check for E_PERM without one of these permissions [$$$]
#  - check for authorize_action and validate_action (by overriding) [edt-34-delete.t 'subclass']
#  - in validate_action, check $edt->get_keyexpr and $edt->get_keylist [edt-34-delete.t 'subclass']
#  - can delete multiple with MULTI_DELETE [edt-34-delete.t 'basic']
#  - can delete multiple with MULTI_DELETE and some keys that are not found [$$$]
#  - check for E_EXECUTE on an SQL statement deliberately created to crash [edt-34-delete.t 'errors']
#  - check for E_EXECUTE on an SQL statement deliberately created to fail

# 15a. delete_cleanup [$$$]
#
# - properly deletes untouched records [edt-35-delete-cleanup.t 'basic']
# - properly deletes no records without error if everything matching the selector has been touched [$$$]
# - check for E_BAD_SELECTOR if no selector is given [$$$]
# - works with 'delete' permission [$$$]
# - works with 'admin' permission [$$$]
# - check for E_PERM without one of these permissions [$$$]
# - check for authorize_action and validate_action (by overriding) [$$$]
# - in before_action, check $edt->get_keyexpr and $edt->get_keylist
# - check for E_EXECUTE on an SQL statement deliberately created to crash [$$$]

# 16. process_record [$$$]
#
#  - can insert and update with process_record, depending on whether or not a key is present
#  - can replace and delete with process_record using the _action field
#  - check that get_record_key works properly aside from process_record

# 17. ignore_record and abort_action [DONE]
#
#  - $edt->ignore_record [edt-14-records.t 'basic']
#  - action and condition labels are properly computed with and without record labels
#	[edt-14-records.t 'basic']
#  - $edt->abort_action [edt-14-records.t 'abort_action']
#  - transaction proceeds if a record with errors is abandoned [edt-14-records.t 'abort_action']
#  - but not in IMMEDIATE_MODE, unless abort_action is called before the action is executed
#	[edt-14-records.t 'abort_action']
#  - error and warning messages from abandoned records are removed [edt-14-records.t 'abort_action']

# 17a. current_action [DONE]
# 
#  - $edt->current_action [edt-12-conditions.t 'basic', edt-13-actions.t 'basic']
#  - $edt->specific_errors [edt-12-conditions.t 'basic']
#  - $edt->specific_warnings [edt-12-conditions.t 'basic']

# 17b. aux_action [$$$]

# 17c. subordinate tables [$$$]

# 18. check_permission [$$$]
#
#  - check that this can be called on an explicitly created action

# 19. initialize_transaction, finalize_transaction, cleanup_transaction [DONE]
#
#  - check that these are called at the right times [edt-20-subclass.t 'initialize and finalize']
#  - check that exceptions thrown by these are properly caught [edt-20-subclass.t 'initialize and finalize']
#  - check that they can do useful work within a transaction [edt-20-subclass.t 'initialize and finalize']

# 20. before_action, after_action, cleanup_action [DONE]
#
#  - check that these are called at the right times [edt-20-subclass.t 'before and after']
#  - check that exceptions thrown by these are properly caught [edt-20-subclass.t 'before and after']
#  - check that they can do useful work within a transaction [edt-20-subclass.t 'before and after']

# 21. reporting methods [$$$]
#
#  - check that the following provide proper values when all operations are done together in one
#    transaction:
# 
#    - inserted_keys [edt-10-transaction.t 'execute']
#    - updated_keys [edt-10-transaction.t 'execute']
#    - replaced_keys [edt-10-transaction.t 'execute']
#    - deleted_keys [edt-10-transaction.t 'execute']
#    - failed_keys
#    - key_labels
#    - label_keys
#    - action_count [edt-10-transaction.t 'execute']
#    - fail_count
#    - has_started [edt-10-transaction.t 'execute']
#    - has_finished [edt-10-transaction.t 'execute']
#    - is_active [edt-10-transaction.t 'execute']
#    - transaction [edt-10-transaction.t 'execute']

# 22. check_permission, check_table_permission, check_record_permission [$$$]
#
#  - make sure these return proper values

# 23. validate_against_schema [$$$]
#  
#  - check that *_no is properly checked as *_id [edt-22-validate.t 'foreign keys']
#  - check that $action->column_special works properly [$$$]
#  - check for E_REQUIRED if a required column is missing [edt-22-validate.t 'required']
#  - check for E_REQUIRED if a required column has the undefined value [edt-22-validate.t 'required']
#  - check for E_REQUIRED if a required column has the empty string [edt-22-validate.t 'required']
# 
# crmod:
#  - check that crmod columns can be set with ALTER_TRAIL and 'admin' [edt-16-modified.t 'crmod admin']
#  - check for E_PERM_COL on crmod columns without ALTER_TRAIL or 'admin' [edt-16-modified.t 'crmod non-admin']
#  - check that both date and date/time are accepted value formats [edt-16-modified.t 'crmod admin']
#  - check for E_FORMAT on crmod columns with improper value format [edt-16-modified.t 'crmod admin']
#  - check that crmod fields are auto-filled correctly on both insert and update [edt-16-modified.t 'crmod']
#
# authent:
#  - check that authent columns can be set with ALTER_TRAIL and 'admin' [edt-16-modified.t 'authent admin']
#  - check for E_PERM_COL on authent columns without ALTER_TRAIL or 'admin' [edt-16-modified.t 'authent non-admin']
#  - check that authent columns can be set with a PBDB::ExtIdent of type PRS. [edt-16-modified.t 'authent admin']
#  - check for E_EXTTYPE for another extident type [edt-16-modified.t 'authent admin']
#  - check that authent columns can be set with an unsigned integer [edt-16-modified.t 'authent admin']
#  - check for E_FORMAT for negative numbers or text or another reference
#  - check for E_KEY_NOT_FOUND if no matching person record is found [$$$]
#  - check that authent fields are auto-filled correctly on both insert and update [edt-16-modified.t 'authent']
#  - check user_id as well as the others [$$$]
#
# admin:
#  - check that admin columns can be set with 'admin' permission
#  - check for E_PERM_COL without 'admin' permission
#  - check that 0 and 1 are accepted as values
#  - check for E_PARAM with other values
# 
# foreign keys: [DONE]
#  - check that foreign key columns accept an unsigned integer [edt-22-validate.t 'foreign keys']
#  - check that foreign key columns accept a PBDB::ExtIdent of the appropriate type
#  	[edt-22-validate.t 'foreign keys']
#  - check for E_EXTTYPE if the extident has an inappropriate type [edt-22-validate.t 'foreign keys']
#  - check for E_KEY_NOT_FOUND if the key does is not found in the foreign table
#  	[edt-22-validate.t 'foreign keys']
#  - check for E_FORMAT for text, negative integer, or other reference type
#  	[edt-22-validate.t 'foreign keys']
#  - check that the empty string or 0 produces 0 [edt-22-validate.t 'foreign keys']
# 
# char and varchar: [DONE]
#  - check that empty string and arbitrary string are accepted [edt-22-validate.t 'text']
#  - check for E_RANGE on length violation [edt-22-validate.t 'text']
# 
# text and tinytext: [DONE]
#  - check that empty string and arbitrary string are accepted [edt-22-validate.t 'text']
#  - check for E_RANGE on length violation [edt-22-validate.t 'text']
# 
# int types: [DONE]
#  - check that boolean fields accept 0 and 1 [edt-22-validate.t 'integer']
#  - check for E_FORMAT on non-numeric values [edt-22-validate.t 'integer']
#  - check for E_RANGE on too large values, both signed and unsigned [edt-22-validate.t 'integer']
#  - check that integer fields accept positive values, 0, and '' [edt-22-validate.t 'integer']
#  - check that '' is entered as null [edt-22-validate.t 'integer']
#  - check that signed integer fields accept negative values [edt-22-validate.t 'integer']
#  - check that values starting with zeros are accepted [edt-22-validate.t 'integer']
#  - check for E_RANGE on size violations for tiny, small, medium, and big [edt-22-validate.t 'integer']
#  - check for E_RANGE on negative size violations for all of the above [edt-22-validate.t 'integer']
#  - check for E_RANGE on negative values for unsigned [edt-22-validate.t 'integer']
#
# decimal types: [DONE]
#  - check that decimal fields accept positive and negative values, 0, and '' [edt-22-validate.t 'fixed']
#  - check that '' is entered as null [edt-22-validate.t 'fixed']
#  - check for E_RANGE on an unsigned field with a negative value [edt-22-validate.t 'fixed']
#  - check for E_RANGE on width violations [edt-22-validate.t 'fixed']
#  - check for E_SIZE on precision violations [edt-22-validate.t 'fixed']
#  - check for W_TRUNC on precision violations on columns with ALLOW_TRUNC property
#	[edt-22-validate.t 'fixed']

# 24. EditTransaction::Condition [DONE]
# 
#  - check that $c->code, $c->label, $c->table, and $c->data work properly [edt-12-conditions.t 'basic']

# 25. EditTransaction::Action: [DONE]
#
# accessor methods: [DONE]
#  - $action->table [edt-13-actions.t 'basic']
#  - $action->operation [edt-13-actions.t 'basic']
#  - $action->label [edt-13-actions.t 'basic']
#  - $action->record [edt-13-actions.t 'basic']
#  - $action->record_value [edt-13-actions.t 'basic']
#  - $action->has_field [edt-13-actions.t 'basic']
#  - $action->permission [edt-13-actions.t 'basic']
#  - $action->keycol [edt-13-actions.t 'basic', 'delete']
#  - $action->keyval [edt-13-actions.t 'basic', 'delete']
#  - $action->column_list [edt-13-actions.t 'basic']
#  - $action->value_list [edt-13-actions.t 'basic']
#  - $action->is_multiple [edt-13-actions.t 'multiple']
#  - $action->action_count [edt-13-actions.t 'multiple']
#  - $action->all_keys [edt-13-actions.t 'multiple']
#  - $action->all_labels [edt-13-actions.t 'multiple']
#  - $action->has_errors [edt-13-actions.t 'basic']
#  - $action->has_warnings [edt-13-actions.t 'basic']
#  - $action->add_error [edt-13-actions.t 'basic']
#  - $action->add_warning [edt-13-actions.t 'basic']
#  - $action->set_attr [edt-13-actions.t 'basic', 'attrs']
#  - $action->get_attr [edt-13-actions.t 'basic', 'attrs']
#  - get_attr called in 'after_action' fetches value from set_attr called in 'before_action'
#	 [edt-13-actions.t 'attrs']
#

# 26. EditTester [DONE]
#
#  $T->new [edt-01-basic.t (initial statements), edt-03-tester.t 'basic']
#  
#  $T->dbh [edt-03-tester.t 'basic']
#  
#  $T->debug [edt-03-tester.t 'basic']
#  
#  $T->create_tables [edt-01-basic.t (initial statements)]
#  
#  $T->set_specific_permission [edt-03-tester.t 'permissions']
#  
#  $T->clear_specific_permissions [edt-03-tester.t 'permissions']
#  
#  $T->new_perm [edt-01-basic.t 'create objects']
#  
#  $T->new_edt [edt-01-basic.t 'create objects']
#  
#  $T->last_edt [edt-03-tester.t 'edts']
#  
#  $T->clear_edt [edt-03-tester.t 'edts']
#  
#  $T->test_permissions [edt-03-tester.t 'test_permissions']
#  
#  $T->ok_result [edt-03-tester.t 'errors', 'warnings']
#   - works with specific edt
#   - reports both errors and warnings
#  
#  $T->ok_no_errors [edt-03-tester.t 'errors']
#   - works with 'current', 'main', 'any'
#   - works with specific edt
#   - works with both error and caution conditions
#   - works with explicit $edt argument
#   - fails when it should
#  
#  $T->ok_has_error [edt-03-tester.t 'errors']
#   - works with 'current', 'main', 'any'
#   - works with specific edt
#   - works with both error and caution conditions
#   - works with explicit $edt argument
#   - fails when it should
#  
#  $T->ok_has_one_error [edt-03-tester.t 'errors']
#   - works with 'current', 'main', 'any'
#   - works with specific edt
#   - works with both error and caution conditions
#   - works with explicit $edt argument
#   - fails when it should
#  
#  $T->diag_errors [edt-03-tester.t 'errors']
#   - works with 'current', 'main', 'any'
#   - works with specific edt
#   - works with both error and caution conditions
#   - works with explicit $edt argument
#  
#  $T->ok_no_warnings [edt-03-tester.t 'warnings']
#   - works with 'current', 'main', 'any'
#   - works with specific edt
#   - works with explicit $edt argument
#   - fails when it should
#  
#  $T->ok_has_warning [edt-03-tester.t 'warnings']
#   - works with 'current', 'main', 'any'
#   - works with specific edt
#   - works with explicit $edt argument
#   - fails when it should
#  
#  $T->ok_no_conditions [edt-03-tester.t 'errors']
#   - works with 'current', 'main', 'any'
#   - works with specific edt
#   - works with explicit $edt argument
#   - fails when it should
#  
#  $T->diag_warnings [edt-03-tester.t 'warnings']
#   - works with 'current', 'main', 'any'
#   - works with specific edt
#   - works with explicit $edt argument
#  
#  $T->ok_found_record [edt-03-tester.t 'records']
#   - works with both single and multiple records
#   - fails when it should
#  
#  $T->ok_no_record [edt-03-tester.t 'records']
#   - works with both single and multiple records
#   - fails when it should
#  
#  $T->ok_count_records [edt-03-tester.t 'records']
#   - works with both single and multiple records
#   - fails when it should
#  
#  $T->clear_table [edt-03-tester.t 'records']
#  
#  $T->fetch_records_by_key [edt-03-tester.t 'records']
#  
#  $T->fetch_records_by_expr [edt-03-tester.t 'records']
#  
#  $T->fetch_keys_by_expr [edt-03-tester.t 'records']
#  
#  $T->fetch_row_by_expr [edt-03-tester.t 'records']
#  
#  $T->inserted_keys [edt-03-tester.t 'keys']
#  
#  $T->updated_keys [edt-03-tester.t 'keys']
#  
#  $T->replaced_keys [edt-03-tester.t 'keys']
#  
#  $T->deleted_keys [edt-03-tester.t 'keys']
#  
#  
#  
#  
#  
#  
#  
#  
