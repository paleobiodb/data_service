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
# ALLOW_KEY_INSERT
# BY_AUTHORIZER
# AUTH_FIELDS
# PRIMARY_KEY
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
# PROCEED
# KEY_INSERT
# MULTI_DELETE
# NO_RECORDS
# ALTER_TRAIL
# DEBUG_MODE
# extra allowance defined by EditTest.pm

# 5. EditTest->new
#
# create with dbh
# create with request
# bad allowance -> W_ALLOW

# 6. Accessor methods
#
# $edt->dbh
# $edt->request
# $edt->transaction
# $edt->role
# $edt->allows

# 7. Debugging
#
# override and capture calls to debug_line

# 8. add_condition
#
# with and without actions extra template defined by EditTest.pm, with data
#   and variation by first data element
# change error to warning with PROCEED
# change error to warning with NOT_FOUND
# parameter errors not changed by NOT_FOUND

# 9. errors and warnings
# 
# $edt->errors
# $edt->warnings
# $edt->generate_msg
# check variable substitution in at least one template

# 10. record labels
#
# check error messages to make sure that record_label is carried through
# check error messages to make sure that labels are properly generated for unlabeled records.

# 11. transaction control
#
# $edt->start_execution
#  - exception when called after committed and aborted transactions
#  - make sure that calling a second time on an active transaction is okay
#       (capture debug_line to make sure)
#  - check for immediate effect with $dbh->select
#  - exception when called if actions have already been specified (use allow instead?)
#  - check with $edt->transaction, $edt->active, and $edt->executing
# 
# $edt->commit
#  - check effect with $dbh->select
#  - exception when called before transaction starts
#  - exception when called after committed and aborted transactions
#  - check with $edt->transaction
#
# $edt->rollback
#  - check effect with $dbh->select
#  - exception when called before transaction starts
#  - exception when called after committed and aborted transactions
#
# $edt->start_transaction
#  - check effect with $dbh->select (no effect until $edt->execute)
#  - exception when called after committed and aborted transactions
#  - make sure that calling a second time on an active transaction is okay
#       (capture debug_line to make sure)
#  - check status with $edt->transaction, $edt->active, and $edt->executing
#
# $edt->execute
#  - check effect with $dbh->select
#  - exception when called after committed and aborted transactions
#  - check status with $edt->transaction, $edt->active, and $edt->executing
#
# authorize_action and validate_action
#  - check status with $edt->transaction, $edt->active, and $edt->executing
#  - check both before and after $edt->start_execution

# 12. insert
#
#  - can insert with CREATE
#  - check for C_CREATE without CREATE
#  - can insert with 'post' permission
#  - can insert with 'admin' permission
#  - check for E_PERM without one of these permissions
#  - check for E_HAS_KEY if a key is given
#  - check for authorize_action and validate_action (by overriding)
#  - in validate_action, check $action->get_keyexpr and $action->get_keylist
#  - check for E_EXECUTE on an SQL statement deliberately created to crash
#  - check for E_EXECUTE on an SQL statement deliberately created to fail

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

# 17. ignore_record and abandon_record
#
#  - labels are properly computed if some records are ignored
#  - rest of transaction proceeds if some records are abandoned
#  - error messages from abandoned records are removed

# 18. check_permission
#
#  - check that this can be called on an explicitly created action

# 19. initialize_transaction and finalize_transaction
#
#  - check that these are called at the right times
#  - check that they can do useful work within a transaction

# 20. before_action and after_action
#
#  - check that these are called at the right times
#  - check that they can do useful work within a transaction

# 21. inserted_keys, updated_keys, replaced_keys, deleted_keys, failed_keys, key_labels, action_count, fail_count
#
#  - check that these all provide proper values when all are done together in one transaction.

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

# 24. error and warning conditions
#
#  - check that $c->code, $c->label, $c->table, and $c->data work properly

# 25. EditAction:
#
# accessor methods:
#  - check $action->table
#  - check $action->operation
#  - check $action->record
#  - check $action->label
#  - check $action->field
#  - check $action->has_field
#  - check $action->permission
#  - check $action->keyval
#  - check $action->column_list
#  - check $action->value_list
#  - check $action->is_multiple
#  - check $action->count
#  - check $action->all_keys
#  - check $action->all_labels
#  - check $action->has_errors
#  - check $action->has_warnings
#  - check $action->set_permission
#  - check $action->set_column_values
#  - check $action->set_keyval
#  - check $action->add_error;
#  - check $action->add_warning;
#  - check $action->set_attr and $action->get_attr, from e.g. before_action and after_action









# two commits in a row
# two aborts in a row
# commit followed by abort
# abort followed by commit
