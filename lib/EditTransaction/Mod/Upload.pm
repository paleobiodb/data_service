# 
# EditTransaction::Mod::Upload
# 
# This module provides routines that are needed for uploading data to The Paleobiology Database.
# 
# Author: Michael McClennen

package EditTransaction::Mod::Upload;

use strict;

use TableDefs qw(%TABLE);

use Carp qw(croak);

use feature 'unicode_strings';

use Role::Tiny;

our (@CARP_NOT) = qw(EditTransaction);

our ($CAN_UPLOAD) = 1;


# check_upload ( table_name, upload_name )
# 
# Check to see if there has already been an upload with the specified name to the
# specified table. Return true if so, false otherwise.

sub check_upload {

    my ($edt, $table_name, $upload_name) = @_;
    
    my $dbh = $edt->dbh;
    
    my $qname = $dbh->quote($upload_name);
    
    my ($check) = $dbh->selectrow_array(<<~END_SQL);
	SELECT `upload` FROM `$TABLE{$table_name}`
	WHERE upload = $qname LIMIT 1
	END_SQL
    
    return $check;
}


