# 
# Paleobiology Data Service version 1.0
# 
# This file defines version 1.0 of the Paleobiology Data Service.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>


package Data_1_0;

# setup ( ds )
# 
# This routine is called from the main program, in order to set up version 1.0
# of the data service.  The main service object is provided as a parameter,
# and we instantiate a sub-service object here.

sub setup {

    my ($ds) = @_;
    
    my $ds0 = $ds->define_subservice(
	{ name => 'data1.0',
	  label => '1.0',
	  path_prefix => 'data1.0',
	  doc_templates => 'doc/1.0' },
	    "I<This version is obsolete, and has been discontinued.>");
}

1;

