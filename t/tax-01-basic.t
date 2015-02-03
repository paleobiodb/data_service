# 
# PBDB LIB Taxonomy.pm
# --------------------
# 
# Test the module Taxonomy.pm.  This file tests that the module can be loaded
# correctly and a new Taxonomy instance generated.


use lib 'lib';
use Test::More tests => 4;

BEGIN {
    
    use_ok("CoreFunction", qw(connectDB configData)) or BAIL_OUT "cannot load module 'CoreFunction.pm'";
    
    use_ok("Taxonomy") or BAIL_OUT "cannot load module 'Taxonomy.pm'";

}


my $dbh;

eval {
    $dbh = connectDB("config.yml");
};

unless ( ok(defined $dbh, "dbh acquired") )
{
    diag("message was: $@");
    BAIL_OUT;
}

my $taxonomy = new_ok("Taxonomy" => [$dbh, 'taxon_trees'], 'taxonomy');

