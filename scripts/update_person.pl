use lib "../cgi-bin";
use DBConnection;
use DBTransactionManager;
$dbh = DBConnection::connect();
$dbt = DBTransactionManager->new($dbh);

open (FH1, "<UPDATE_PERSON_AUTH")
    or die "CAN NOT OPEN authorizers";
open (FH2, "<UPDATE_PERSON_ENT")
    or die "CAN NOT OPEN enterers";


if ($ARGV[0] eq '--do_sql') {
    $doUpdates = 1;
    print "RUNNING SQL\n";
} else {
    print "DRY RUN\n";
}



@P = <FH1>;
@E = <FH2>;

foreach $line (@P,@E) {
    ($first,$last,$institution,$email) = parseLine($line);
    if ($first) {
        $init = substr($first,0,1).". $last";
        $init =~ s/ü/u/g;
        $INST = "";
        if ($institution) {
            $INST = "institution=".$dbh->quote($institution).", ";
        }
        $EML = "";
        if ($email) {
            if ($email =~ /unknown/) {
                $EML = "email='', ";
            } else {
                $EML = "email=".$dbh->quote($email).", ";
            }
        }
        $sql = "SELECT * FROM person WHERE name LIKE ".$dbh->quote($init);
        my $p = ${$dbt->getData($sql)}[0];
        if (!$p) {
            print "WARNING: NOT UPDATING '$init', not in the DB\n";
        }
        if ($p->{'institution'} && $p->{'institution'} ne $institution) {
            print "WARNING: insititution differs DB: $p->{institution} PAGE $institution\n";
        }
        if ($p->{'email'} && $p->{'email'} ne $email) {
            print "WARNING: email differs DB: $p->{email} PAGE $email\n";
        }
        
        $sql = "UPDATE person SET modified=modified,last_action=last_action, $INST $EML first_name=".$dbh->quote($first).", last_name=".$dbh->quote($last). " WHERE name LIKE ".$dbh->quote($init); 
        print "$sql\n";
        if ($doUpdates) {
            $dbh->do($sql);
        }
    } else {
        print "ERROR: CANNOT PARSE LINE: $line\n";
    }
}

@sqls = (
"UPDATE person SET modified=modified,last_action=last_action, institution='Universit&auml;t W&uuml;rzbug', first_name='Franz', last_name='Fürsich' WHERE name LIKE 'F. Fursich'",
"UPDATE person SET modified=modified,last_action=last_action, institution='Harvard University', first_name='Sofy', last_name='Low' WHERE name LIKE 'S. Low'",
"UPDATE person SET modified=modified,last_action=last_action, institution='Universit&auml;t Erlangen', first_name='Alexander', last_name='Nützel' WHERE name LIKE 'A. Nuetzel'",
"UPDATE person SET modified=modified,last_action=last_action, institution='University of Rajasthan', first_name='Dhirendra Kumar', last_name='Pandey' WHERE name LIKE 'D. Pandey'",
"UPDATE person SET modified=modified,last_action=last_action, institution='University of Chicago', first_name='Jack', last_name='Sepkoski' WHERE name LIKE 'J. Sepkoski'",
"UPDATE person SET modified=modified,last_action=last_action, institution='Virginia Tech', first_name='Elizabeth', last_name='Kowalski' WHERE name LIKE 'E. Kowalski'",
"UPDATE person SET modified=modified,last_action=last_action, institution='Universit&auml;t Witten/Herdecke', first_name='Susanna', last_name='Kümmell' WHERE name LIKE 'S. Kuemmell'",
"UPDATE person SET modified=modified,last_action=last_action, institution='University of Georgia', first_name='Karen', last_name='Bezusko-Layou' WHERE name LIKE 'K. Bezusko-Layou'",
"UPDATE person SET modified=modified,last_action=last_action, institution='Museum f&uuml;r Naturkunde, Berlin', first_name='Sabine', last_name='Nürnberg' WHERE name LIKE 'S. Nurnberg'",
"UPDATE person SET modified=modified,last_action=last_action, institution='University College London', first_name='Seb', last_name='Perceau Wells' WHERE name LIKE 'S. Perceau Wells'",
"UPDATE person SET modified=modified,last_action=last_action, institution='State University of New York, Stony Brook', first_name='Joe', last_name='Rezza' WHERE name LIKE 'J. Rezza'",
"UPDATE person SET modified=modified,last_action=last_action, institution='University of Chicago', first_name='Kevin', last_name='Boyce', email='ckboyce\@uchicago.edu' WHERE name LIKE 'C. Boyce'",
"UPDATE person SET modified=modified,last_action=last_action, institution='Binghamton University', first_name='Bill', last_name='Stein' WHERE name LIKE 'W. Stein'",
"UPDATE person SET modified=modified,last_action=last_action, institution='Smithsonian Institution', email='valiulis\@nmnh.si.edu', first_name='Elizabeth', last_name='Valiulis' WHERE name LIKE 'E. Valiulis'",
"UPDATE person SET modified=modified,last_action=last_action, first_name='Cinzia', last_name='Cervato' WHERE name LIKE 'C. Cervato'",
"UPDATE person SET modified=modified,last_action=last_action, institution='Colby College', first_name='Robert', last_name='Gastaldo' WHERE name LIKE 'R. Gastaldo'",
"UPDATE person SET modified=modified,last_action=last_action, institution='Museum f&uuml;r Naturkunde, Berlin',first_name='David', last_name='Lazarus' WHERE name LIKE 'D. Lazarus'",
"UPDATE person SET modified=modified,last_action=last_action, institution='University of California, Santa Barbara', first_name='David', last_name='Lamb' WHERE name LIKE 'D. Lamb'",
"UPDATE person SET modified=modified,last_action=last_action, first_name='Vladimir', last_name='Davydov' WHERE name LIKE 'V. Davydov'",
"UPDATE person SET modified=modified,last_action=last_action, institution='University of Chicago', first_name='Lee Hsiang', last_name='Liow',institution='University of Chicago' WHERE name LIKE 'L. Liow'"
);

foreach my $sql (@sqls) {
    print "$sql\n";
    if ($doUpdates) {
        $dbh->do($sql);
    }
}



sub parseLine {
#    <tr><td>Phil Borkow     <td>Penn State  <td>borkowosu@yahoo.com
    my $l = shift;
    my ($first,$last,$inst,$email) = ("","","","");
    if ($l =~ /^<tr><td>([ü\w'-]+)\s+([ü\w'-]+)\s*<td>(.*?)\s*<td>(.*?)\s*$/) {
        ($first,$last,$institution,$email) = ($1,$2,$3,$4);
    }
    return ($first,$last,$institution,$email);
}

