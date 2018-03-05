#
#



# Things to test:
#
# 1. Table properties:
#
# ALLOW_POST
# ALLOW_VIEW
# ALLOW_EDIT
# ALLOW_DELETE
# ALLOW_REPLACE
# ALLOW_KEY_INSERT
# BY_AUTHORIZER
# AUTH_FIELDS
# PRIMARY_KEY [edt-01-basic.t 'create objects']
# PRIMARY_ATTR

# 2. Column properties:
#
# ID_TYPE
# REQUIRED
# ADMIN_SET

# 3. Table roles:
#
# admin - can insert, update, delete
# authorized - can insert, update, delete
# enterer - can insert, update, delete

# 4. Allowances: 
#
# CREATE
# MULTI_DELETE
# ALTER_TRAIL
# DEBUG_MODE [edt-01-basic.t 'debug output']
# SILENT_MODE [edt-01-basic.t 'debug output']
# IMMEDIATE_MODE [edt-10-transaction.pm 'immediate']
# PROCEED_MODE [edt-11-proceed.pm 'proceed_mode']
# NOT_FOUND [edt-11-proceed.pm 'not_found']
# NO_RECORDS [edt-11-proceed.pm 'no_records']
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
# override and capture calls to debug_line [edt-01-basic.t 'debug output']

# 8. errors, warnings, and cautions [DONE]
# 
#  $edt->add_condition [edt-12-conditions.t 'basic']
#  - works before first action, works after actions [edt-12-conditions.t 'basic']
#  - works with specific actions [edt-12-conditions.t 'basic']
#  - works with both undef and 'main' as first argument [edt-12-conditions.t 'basic']
#  - throws exception if bad code is given [edt-12-conditions.t 'register']
#  - errors are demoted to warnings with PROCEED_MODE
#	[edt-11-proceed.t 'proceed_mode', edt-12-conditions.t 'proceed']
#  - E_NOT_FOUND is demoted to warning with NOT_FOUND
#	[edt-11-proceed.t 'not_found', edt-12-conditions.t 'notfound']
#
#  $edt->register_condition [edt-12-conditions.t 'register']
#  - throws exception if code does not match proper pattern [edt-12-conditions.t 'register']
# 
#  $edt->errors [edt-12-conditions.t 'basic']
#  $edt->specific_errors [edt-12-conditions.t 'basic']
#  $edt->error_strings [edt-12-conditions.t 'basic']
#  $edt->warnings [edt-12-conditions.t 'basic']
#  $edt->specific_warnings [edt-12-conditions.t 'basic']
#  $edt->warning_strings [edt-12-conditions.t 'basic']
#  $edt->generate_msg [edt-12-conditions.t 'generate_msg']
#
#  variable substitution in templates [edt-12-conditions.t 'templates']
#  selection of template by first parameter [edt-12-conditions.t 'templates']
#  
#  conditions work properly from the following overrideable methods:
#   - authorize_action [edt-30-subclass.t 'authorize']
#   - validate_action [edt-30-subclass.t 'validate']
#   - before_action [edt-30-subclass.t 'before and after']
#   - after_action [edt-30-subclass.t 'before and after']
#   - cleanup_action [edt-30-subclass.t 'before and after']
#   - initialize_transaction [edt-30-subclass.t 'initialize and finalize']
#   - finalize_transaction [edt-30-subclass.t 'initialize and finalize']
#   - cleanup_transaction [edt-30-subclass.t 'initialize and finalize']

# 10. record labels [$$$]
# 
#  - record label is carried through properly to conditions and messages [edt-12-condition.t 'basic']
#  - labels are properly generated for unlabeled records [edt-14-records.t 'basic']
#  - $edt->key_labels

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
#	[edt-30-subclass.t 'authorize', 'validate']
#  - check both before and after $edt->start_execution [edt-10-transaction.t 'errors']
# 
# general [DONE]
# - automatic rollback when object goes out of scope [edt-01-basic.t 'out of scope']

# 12. insert_record
# 
#  - can insert with CREATE [edt-10-transaction.t 'basic']
#  - check for C_CREATE without CREATE [edt-20-insert.t 'basic']
#  - can insert with 'post' permission
#  - can insert with 'admin' permission
#  - check for E_PERM without one of these permissions
#  - check for E_HAS_KEY if a key is given
#  - check for authorize_action and validate_action (by overriding)
#  - in validate_action, check $action->get_keyexpr and $action->get_keylist
#  - an SQL statement deliberately created to crash results in E_EXECUTE
#	[edt-11-proceed.t 'proceed_mode', edt-30-subclass.t 'before and after']

# 13. update
#
#  - can update with a key
#  - check for E_NO_KEY without one
#  - check for E_NOT_FOUND if key does not exist in table
#  - can update with 'edit' permission
#  - can update with 'admin' permission
#  - check for E_PERM without one of these permissions
#  - check for authorize_action and validate_action (by overriding)
#  - in validate_action, check $action->get_keyexpr and $action->get_keylist
#  - check for E_EXECUTE on an SQL statement deliberately created to crash
#  - check for E_EXECUTE on an SQL statement deliberately created to fail

# 14. replace
# 
#  - can replace with a key
#  - check for E_NO_KEY without one
#  - check for E_NOT_FOUND if key does not exist in table and CREATE is not allowed
#  - can replace with 'post' permission and CREATE
#  - can replace with 'admin' permission and CREATE
#  - check for E_PERM without one of these permissions
#  - can replace with 'edit' permission and key found
#  - can replace with 'admin' permission and key found
#  - check for E_PERM without one of these permissions
#  - check for authorize_action and validate_action (by overriding)
#  - in validate_action, check $action->get_keyexpr and $action->get_keylist
#  - check for E_EXECUTE on an SQL statement deliberately created to crash
#  - check for E_EXECUTE on an SQL statement deliberately created to fail

# 15. delete
#
#  - can delete with a key
#  - check for E_NO_KEY without one
#  - check for E_NOT_FOUND if key does not exist in table
#  - can delete with 'delete' permission
#  - can delete with 'admin' permission
#  - check for E_PERM without one of these permissions
#  - check for authorize_action and validate_action (by overriding)
#  - in validate_action, check $edt->get_keyexpr and $edt->get_keylist
#  - can delete multiple with MULTI_DELETE
#  - can delete multiple with MULTI_DELETE and some keys that are not found
#  - in validate_action, check $edt->get_keylist with MULTI_DELETE
#  - check for E_EXECUTE on an SQL statement deliberately created to crash
#  - check for E_EXECUTE on an SQL statement deliberately created to fail

# 16. insert_update
#
#  - can insert and update with insert_update, depending on whether or not a key is present
#  - check that get_record_key works properly aside from insert_update

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

# 17b. aux_action

# 18. check_permission
#
#  - check that this can be called on an explicitly created action

# 19. initialize_transaction, finalize_transaction, cleanup_transaction [DONE]
#
#  - check that these are called at the right times [edt-30-subclass.t 'initialize and finalize']
#  - check that exceptions thrown by these are properly caught [edt-30-subclass.t 'initialize and finalize']
#  - check that they can do useful work within a transaction [edt-30-subclass.t 'initialize and finalize']

# 20. before_action, after_action, cleanup_action [DONE]
#
#  - check that these are called at the right times [edt-30-subclass.t 'before and after']
#  - check that exceptions thrown by these are properly caught [edt-30-subclass.t 'before and after']
#  - check that they can do useful work within a transaction [edt-30-subclass.t 'before and after']

# 21. reporting methods
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
#    - action_count [edt-10-transaction.t 'execute']
#    - fail_count
#    - has_started [edt-10-transaction.t 'execute']
#    - has_finished [edt-10-transaction.t 'execute']
#    - is_active [edt-10-transaction.t 'execute']
#    - transaction [edt-10-transaction.t 'execute']

# 22. check_permission, check_table_permission, check_record_permission
#
#  - make sure these return proper values

# 23. validate_against_schema
#
#  - check that *_no is properly checked as *_id
#  - check that $action->column_skip_validate works properly
#  - check that columns with an undefined value are ignored
#  - check for E_REQUIRED if a required column is missing
#  - check for E_REQUIRED if a required column has the undefined value
#  - check for E_REQUIRED if a required column has the empty string
#
# crmod:
#  - check that crmod columns can be set with ALTER_TRAIL and 'admin'
#  - check for E_PERM_COL on crmod columns without ALTER_TRAIL or 'admin'
#  - check that both date and date/time are accepted value formats
#  - check for E_PARAM on crmod columns with improper value format
#  - check that crmod fields are auto-filled correctly on both insert and update
#
# authent:
#  - check that authent columns can be set with ALTER_TRAIL and 'admin'
#  - check for E_PERM_COL on authent columns without ALTER_TRAIL or 'admin'
#  - check that authent columns can be set with a PBDB::ExtIdent of type PRS.
#  - check for E_PARAM for another extident type
#  - check that authent columns can be set with an unsigned integer
#  - check for E_PARAM for negative numbers or text or another reference
#  - check for E_KEY_NOT_FOUND if no matching person record is found
#  - check that authent fields are auto-filled correctly on both insert and update
#  - check user_id as well as the others
#
# admin:
#  - check that admin columns can be set with 'admin' permission
#  - check for E_PERM_COL without 'admin' permission
#  - check that 0 and 1 are accepted as values
#  - check for E_PARAM with other values
# 
# foreign keys:
#  - check that foreign key columns accept an unsigned integer
#  - check that foreign key columns accept a PBDB::ExtIdent of the appropriate type
#  - check for E_PARAM if the extident has an inappropriate type
#  - check for E_KEY_NOT_FOUND if the key does is not found in the foreign table
#  - check for E_PARAM for text, negative integer, or other reference type
#  - check that the empty string or 0 produces 0
#
# char and varchar:
#  - check that empty string and arbitrary string are accepted
#  - check for E_PARAM on length violation
#
# text and tinytext:
#  - check that empty string and arbitrary string are accepted
#  - check for E_PARAM on length violation
#
# int types:
#  - check that boolean fields accept 0 and 1
#  - check for E_PARAM on empty string or character values or larger numerics
#  - check that integer fields accept positive values, 0, and ''
#  - check that signed integer fields accept negative values
#  - check that values starting with zeros are accepted
#  - check for E_PARAM on size violations for tiny, small, medium, and big
#  - check for E_PARAM on negative size violations for all of the above
#  - check for E_PARAM on negative values for unsigned
#  - check for E_PARAM on badly formatted values
#
# decimal types:
#  - check that decimal fields accept positive and negative values, 0, and ''
#  - check for E_PARAM on an unsigned field with a negative value
#  - check for E_PARAM on width and precision violations on columns with the STRICT property
#  - check for W_PARAM on width and precision violations on columns without STRICT

# 24. EditTransaction::Condition [DONE]
# 
#  - check that $c->code, $c->label, $c->table, and $c->data work properly [edt-12-conditions.t 'basic']

# 25. EditTransaction::Action: [DONE]
#
# accessor methods:
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
# 


