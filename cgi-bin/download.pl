#!/usr/local/bin/perl

use lib qw(.);
use strict;	

# CPAN modules
use CGI;
use DBTransactionManager;
use URI::Escape;

# Create the CGI, Session, and some other objects.
my $q = new CGI;

# Make a Transaction Manager object
my $dbt = new DBTransactionManager;

my $what = $q->param('what') || 'upload';
my $id = int($q->param('download_id'));

if ($id && $what eq 'upload') {
    my $sql = "SELECT file_name,file_data FROM uploads WHERE upload_id=$id";
    my $row = ${$dbt->getData($sql)}[0];
    if ($row) {
        my $mime_type = "excel/ms-excel";
        my $file_name = $row->{'file_name'};
        $file_name =~ s/"//;
        print $q->header(
            '-Content-disposition'=>"attachment; filename=\"$file_name\"",
            '-Content-type'=> "$mime_type; name=\"$file_name\""
        );
        print $row->{'file_data'};
    }
} else {
    print $q->header('-type'=> "text/html");
}
