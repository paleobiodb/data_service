#!/usr/bin/perl

# created by rjp, 1/2004.
# Represents a single taxon, usually from the authorities table
# (if it doesn't exist in the authorities table, then not all methods will work)

package Taxon;

use strict;
use DBI;
use DBConnection;
use SQLBuilder;
use CGI::Carp qw(fatalsToBrowser);
use Class::Date qw(date localdate gmdate now);
use Errors;


use URLMaker;
use Reference;


use fields qw(	taxonName
				taxonNumber
				taxonRank
				
				taxaHash
				
				SQLBuilder

							);  # list of allowable data fields.

# taxonName is the name of the original taxon the user set
# taxonNumber is the number for the original taxon
# taxonRank is the rank of this taxon, note, this won't be populated until the user calls rank()
# taxaHash is a hash of taxa numbers and ranks.
				

				
				
# includes the following public methods
# -------------------------------------
#
# (void) setWithTaxonNumber(int number)
# (void) setWithTaxonName(string name)
#
# (int) taxonNumber()
# (string) rank()
#
# (string) nameForRank(string rank)
# (int) numberForRank(string rank)
# (int) originalCombination
# (\@) listOfChildren()
#

sub new {
	my $class = shift;
	my Taxon $self = fields::new($class);
	
	# set up some default values
	#$self->clear();	

	return $self;
}


# sets the inital taxon with the taxon_no from the database.
sub setWithTaxonNumber {
	my Taxon $self = shift;
	
	if (my $input = shift) {
		# now we need to get the taxonName from the database if it exists.
		my $tn = $self->getTaxonNameFromNumber($input);
		
		$self->{taxonNumber} = $input;

		if ($tn) {
			# if we found a taxon_name for this taxon_no, then 
			# set the appropriate fields
			$self->{taxonName} = $tn;
		}
	}
}


# Sets the initial taxon with the taxon_name from the database.
# If the taxon is not in the database, then it just sets the name,
# but not the number.
#
# returns a boolean, 1 if it worked, 0, if it couldn't.
sub setWithTaxonName {
	my Taxon $self = shift;
	my $newname;
	
	if (my $input = shift) {
		
		$self->{taxonName} = $input;
		
		# now we need to get the taxonNo from the database if it exists.
		my ($tn, $newname) = $self->getTaxonNumberFromName($input);
		
		if ($tn) {
			# if we found a taxon_no for this taxon_name, then 
			# set the appropriate fields
			$self->{taxonNumber} = $tn;
			$self->{taxonName} = $newname;
		
			return 1;	# it worked
		}	
	}
	
	return 0;
}	


# for internal use only!
# returns the SQL builder object
# or creates it if it has not yet been created
# NOTE *** Be very careful not to call this whithin a method
# which is *already* using SQLBuilder.. For example if you call
# a method from a loop which itself calls  
# getSQLBuilder, then you have a problem.
sub getSQLBuilder {
	my Taxon $self = shift;
	
	my $SQLBuilder = $self->{SQLBuilder};
	if (! $SQLBuilder) {
		$SQLBuilder = SQLBuilder->new();
	}
	
	return $SQLBuilder;
}


# for internal use only - get the name for a taxon number
# return empty string if it can't find the name.
sub getTaxonNameFromNumber {
	my Taxon $self = shift;
	
	if (my $input = shift) {

		my $sql = $self->getSQLBuilder();
		my $tn = $sql->getSingleSQLResult("SELECT taxon_name FROM authorities 
				WHERE taxon_no = $input");

		if ($tn) {
			return $tn;
		}
	}
	
	return "";
}


# **For internal use only - get the number of a taxon from the name
# returns an ARRAY with two elements: the taxon_no, and the new taxon_name.
# This is done because the taxon_name may be shortened, for example, if
# it's a genus species pair, but we only have an entry for the genus, not the pair.
# returns 0 if it can't find the number. 
# Note, not all taxa are in this table, so it won't work for something that dosen't exist.
#
# Note, this also won't work very well for homonyms, etc.. For example, if two entries exist
# in the authorities table with the same taxon_name, then we'll always just grab the first one.
# This doesn't really make much sense, but it's the best we can do for now.
sub getTaxonNumberFromName {
	my Taxon $self = shift;

	if (my $input = shift) {
		my $sql = $self->getSQLBuilder();
		
		my $tn = $sql->getSingleSQLResult("SELECT taxon_no FROM authorities WHERE taxon_name = '$input'");
		
		if ($tn) {
			return ($tn, $input);
		}
		
		# if we make it to here, then that means that we didn't find the
		# taxon in the authorities table, so try it with just the first part
		# (ie, we'll assume that it was a genus species, and cut off the species)
		$input =~ s/^(.*)\s.*$/$1/;
		
		my $tn = $sql->getSingleSQLResult("SELECT taxon_no FROM authorities WHERE taxon_name = '$input'");
		
		if ($tn) {
			return ($tn, $input);
		}
	}
	
	return (0, "");
}






# return the taxonNumber for the originally specified taxon.
sub taxonNumber {
	my Taxon $self = shift;

	return $self->{taxonNumber};	
}


# return the taxonName for the initially specifed taxon.
sub taxonName {
	my Taxon $self = shift;

	return $self->{taxonName};	
}


# returns the rank of the taxon this object represents,
# for example, class, family, order, genus, species, etc.
sub rank {
	my Taxon $self = shift;

	if (! $self->{taxonRank}) {	
		# if we haven't already fetched the rank, then fetch it.
		my $sql = $self->getSQLBuilder();

		my $r = $sql->getSingleSQLResult("SELECT taxon_rank FROM authorities WHERE taxon_no = $self->{taxonNumber}");
	
		$self->{taxonRank} = $r;	
	}

	return $self->{taxonRank};
}


# pass this a rank such as "family", "class", "genus", etc. and it will
# return the name of the taxon at that rank as determined by the taxaHash.
sub nameForRank {
	my Taxon $self = shift;
	my $key = shift; 
	
	if (! ($self->{taxaHash}) ) {
		# if the hash doesn't exist, then create it
		$self->createTaxaHash();
	}

	
	my $hash = $self->{taxaHash};
	my %hash = %$hash;
	
	my $id = $hash{$key};
	
	if ($id) {
		# now we need to get the name for it
		my $sql = $self->getSQLBuilder();
		return $sql->getSingleSQLResult("SELECT taxon_name FROM authorities WHERE taxon_no = $id");
	}
	
	return "";
}

# same as nameForRank(), but returns a taxon_no.
sub numberForRank {
	my Taxon $self = shift;
	my $key = shift; 
	
	if (! ($self->{taxaHash}) ) {
		# if the hash doesn't exist, then create it
		$self->createTaxaHash();
	}

	my $hash = $self->{taxaHash};
	my %hash = %$hash;
	
	return $hash{$key};
}


# returns a URL string for the taxon name which links to the 
# checkTaxonInfo routine.
sub URLForTaxonName {
	my Taxon $self = shift;
	
	my $rank = $self->rank();
	my $name = $self->taxonName();
	
	my $url = "bridge.pl?action=checkTaxonInfo&taxon_name=$name&taxon_rank=";
	
	if ($rank eq 'species') {
		$url .= 'Genus+and+species';
	} elsif ($rank eq 'genus') {
		$url .= 'Genus';
	} else {
		$url .= 'Higher+taxon';	
	}
	
	return URLMaker::escapeURL($url);
}


# for the taxon the user set with setTaxonNumber/Name(), 
# finds the original combination of this taxon (ie, genus and species).
# Note, if the current taxon doesn't have an entry in the opinions table,
# it will just return the taxon number we started with (the one the user originally set).
#
# returns a taxon_no (integer).
# if it can't find one, returns 0 (false).
sub originalCombination {
	my Taxon $self = shift;
	
	my $sql = $self->getSQLBuilder();
	
	my $tn = $self->taxonNumber();  # number we're starting with
	
	
	my $cn = $sql->getSingleSQLResult("SELECT child_no FROM opinions WHERE parent_no = $tn 
		AND status IN ('recombined as', 'corrected as')");
	
	if (! $cn) {
		return $tn;		# return the number we started with if there are no recombinations	
	}
	
	return $cn;
}



# for internal use only
# creates a hash of all taxa ranks and numbers
# for the original taxa the user passed in.
# Note, only goes *up* to higher taxa, not down.
sub createTaxaHash {
	my Taxon $self = shift;

	my $sql = $self->getSQLBuilder();
	
	# first go up the hierarchy from the passed in taxon
	# ie, go to the parent of the passed in taxon
	
	# get the initial taxon the user set
	my $tn = $self->taxonNumber();
	if (! $tn) { return };
	
	my %hash;  # hash of the results
	
	my $ref_has_opinion;  # boolean
	my ($pubyr, $idNum);
	my $resultRef;  # sql query results
	
	# another sql object for executing subqueries.
	my $subSQL = SQLBuilder->new();
	
	# first, insert the current taxon into the hash
	my $ownTaxonRank = $subSQL->getSingleSQLResult("SELECT taxon_rank FROM 
								authorities WHERE taxon_no = $tn");
	
	$hash{$ownTaxonRank} = $tn;

	# go up the hierarchy to the top (kingdom)
	# from the rank the user started with.
	while ($tn) {

		# note, the "ORDER BY o.parent_no DESC" is important - this means that if we have two
		# rows with the same pubyr, then it will always fetch the last one added since 
		# the numbers increment on each addition.
		$sql->setSQLExpr("SELECT o.parent_no, o.pubyr, o.ref_has_opinion, 
		o.reference_no, r.pubyr FROM opinions o, refs r 
		WHERE o.child_no = $tn AND o.reference_no = r.reference_no 
		AND status = 'belongs to' ORDER BY o.parent_no DESC");
		$sql->executeSQL();

		
		# loop through all result rows, and find the one with the most
		# recent pubyr.  Note, we'll have to look at the reference if the ref_has_opinion field is true.
		$pubyr = 0;
		$idNum = 0;
		
		my $tempYR;
		while ($resultRef = $sql->nextResultArrayRef()) {

			# note, there is a special case where the parent_no = child_no.  This should *never* happen,
			# but due to some older errors, it does seem to happen once in a while.  In this case,
			# we should just abort.
			if ($resultRef->[0] == $tn) {
				last;  # exit the loop
			}
		
			
			if ($ref_has_opinion = $resultRef->[2]) {
				# if ref_has_opinion is YES, then we need to look to the reference
				# to find the pubyr
				$tempYR = $resultRef->[4]; # pubyr from reference
			} else {
				$tempYR = $resultRef->[1];  # pubyr from opinion
			}
				
			if ($tempYR > $pubyr) {
				$pubyr = $tempYR;
				$idNum = $resultRef->[0];
			}
			
		} # end while $resultRef.

		$sql->finishSQL();
		my $parent = $idNum;  # this is the parent with the most recent pubyr.

		# get the rank of the parent
		my $pRank = $sql->getSingleSQLResult("SELECT taxon_rank FROM authorities WHERE taxon_no = $parent");
		
		# insert it into the hash, so we have the parent rank as the key
		# and the parent number as the value.
		$hash{$pRank} = $parent;
		
		# also insert the pubyr for this parent, keyed to the id number.
		$hash{$parent} = $pubyr;		
					
		$tn = $parent;
		#print "tn = $tn, id = $idNum, pubyr = $pubyr\n";
	}
	

	#store the hash in the object data field
	$self->{taxaHash} = \%hash;
}



# Returns a reference to a sorted array of the "children" to this taxon, ie, the ones
# right below it in ranking.  Each element is a taxon object.
# Note, for now, this just looks at the opinions and
# authorities table, so if a taxon exists in an occurrence, but doesn't exist in the
# authorities table, it won't be listed.  Also, note that for children of a genus 
# (ie, species), it will also do a pattern match for the taxon_name in the authorities
# table so that taxa will be included in the list for which opinions don't exist.  Ie,
# if a taxon exists in the authorities table, but doesn't have any opinions about it,
# then it will still be listed.
#
# For example, if the taxon is a Genus, it will return a list of species.
#  
sub listOfChildren {
	my Taxon $self = shift;

	my $sql = $self->getSQLBuilder();
	my $rank = $self->rank(); # rank of the parent taxon
	my $tn = $self->{taxonNumber};  # number of the parent taxon
	my $tname = $self->taxonName();
	
	$sql->setSQLExpr("SELECT DISTINCT taxon_name, taxon_no
			   		FROM opinions, authorities
					WHERE parent_no = $tn 
					AND status = 'belongs to' AND child_no = taxon_no 
					ORDER BY taxon_name");
	
	my $results = $sql->allResultsArrayRef();
	
	# build up a hash of taxa names so we can more easily insert the addional
	# names that we'll fetch below..
	
	my %taxaHash;
	foreach my $r (@$results) {
		$taxaHash{$r->[0]} = $r->[1];  # key is name, value is taxon_no	
	}
	
	# if the parent rank is a genus, then we will also look through the authorities
	# table for taxa starting with the genus name, but possibly which don't have
	# opinions about them.
	if ($rank eq 'genus') {
		# note, the space after in '$tname \%' is important.
		$sql->setSQLExpr("SELECT DISTINCT taxon_name, taxon_no FROM authorities 
			WHERE taxon_name LIKE '$tname \%' ORDER BY taxon_name");
	
		$results = $sql->allResultsArrayRef();
	
		foreach my $r (@$results) {
			$taxaHash{$r->[0]} = $r->[1]; # key is name, value is taxon_no
		}		
	}
	
	# at this point, we should have a hash which contains keys for each taxa to display
	# so, get the keys, sort them, and we're done.
	
	my @taxa;
	my @taxaKeys = keys(%taxaHash);
	foreach my $k (@taxaKeys) {
		my $newT = Taxon->new();
		$newT->setWithTaxonNumber($taxaHash{$k});
		push(@taxa, $newT);  # add the new taxon object onto the taxa array
	}
	

	# at this point, we have an array of taxon objects.
	# so, we'll sort it based on taxon_name
	@taxa = sort { $a->taxonName() cmp $b->taxonName() } @taxa;
	return \@taxa;
}



# debugging
sub printTaxaHash {
	my Taxon $self = shift;
	my $hash = $self->{taxaHash};
	my %hash = %$hash;
	print "Printing Taxa Hash\n";
	print "hash = '$hash'";
	
	# print out for debugging purposes.
	my @keys = keys(%hash);
	foreach my $key (@keys) {
		print "key = $key\n";
	}
	
	
	
		# print out for debugging purposes.
#	my @keys = keys(%hash);
#	foreach my $key (@keys) {
#		$sql->clear();
#		
#		if (!($key =~ /\d/)) {
#			my $taxon_no = $hash{$key};
#		
#			$sql->setSQLExpr("SELECT taxon_name FROM authorities WHERE taxon_no = '$taxon_no'");
#			$sql->executeSQL();
#			my @result = $sql->nextResultArray();
#			my $taxon_name = $result[0];
#			print "$key = $taxon_name\n";
#		}
#	}
	# end of printing section for debugging

}



# mainly meant for internal use
# returns a hashref with all data (select *) for the current taxon,
# *if* we have a taxon_no for it.  If we don't, then it returns nothing.
sub databaseRecord {
	my Taxon $self = shift;

	if (! $self->{taxonNumber}) {
		return;	
	}
	
	my $sql = $self->getSQLBuilder();
	my $dbh = $sql->dbh();
	
	# grab all the current data for this taxon from the database	
	my $sth = $dbh->prepare("SELECT * FROM authorities WHERE taxon_no = ?");
	$sth->execute($self->{taxonNumber});

	return $sth->fetchrow_hashref(); 	
}



# Pass this an HTMLBuilder object
# and a session object.  
#
# Optionally, pass it the CGI object ($q), but usually this is only done
# if we're running the displayAuthorityForm for the second time (ie, if they
# had errors the first time).
# 
# Displays the form which allows users to enter/edit authority
# table data.
#
# rjp, 3/2004
sub displayAuthorityForm {
	my Taxon $self = shift;
	my $hbo = shift;
	my $s = shift;
	my $q = shift;
	
	my %fields;  # a hash of fields and values that we'll pass to HTMLBuilder to pop. the form.
	my @nonEditables; 	# fields that the user can't edit.
	
	
	my $secondTime = 0;  # is this the second time displaying this form?
	if (($q) && ($q->{second_submission}) ) {
		# this means that we're displaying the form for the second
		# time with some error messages.
		$secondTime = 1;
			
		# so grab all of the fields from the CGI object
		# and stick them in our fields hash.
		my $fieldRef = Globals::copyCGIToHash($q);
		%fields = %$fieldRef;
	}
	
	Debug::dbPrint("second_submission = $secondTime");
	Debug::dbPrint("fields, second sub = " . $fields{second_submission});
	
	
	# is this a new entry, or an edit of an old record?
	my $isNewEntry = 1;  
	if ($self->{taxonNumber}) {
		$isNewEntry = 0;  # it must exist if we have a taxon_no for it.
	}
	
	
	# if the taxon is already in the authorities table,
	# then grab the data from that table row.
	# (unless we're going through for the second time);
	if ((! $isNewEntry) && (!$secondTime) ) {	
		my $results = $self->databaseRecord();
		%fields = %$results;		
	 	Debug::dbPrint("querying database");
		
		$fields{taxon_no} = $self->{taxonNumber};
	}
	
	$fields{taxon_name} = $self->{taxonName};
	if (! $secondTime) {  # otherwise the taxon_name_corrected will already have been set.
		$fields{taxon_name_corrected} = $self->{taxonName};
	}
	
	# if the type_taxon_no exists, then grab the name for that taxon.
	if ((!$secondTime) && ($fields{type_taxon_no})) {
		$fields{type_taxon_name} = $self->getTaxonNameFromNumber($fields{type_taxon_no});
	}
	
	
	# populate the correct pages/figures fields depending
	# on the ref_is_authority value.

	my $ref = Reference->new();
	
	if ($isNewEntry) {
		# for a new entry, use the current reference from the session.
		$ref->setWithReferenceNumber($s->currentReference());
	} else {
		# for an old entry, use the reference_number in the record. 
		$ref->setWithReferenceNumber($fields{reference_no});
	}
	
	$fields{primary_reference} = $ref->formatAsHTML();
	
	if ($fields{'ref_is_authority'} eq 'YES') {
		# reference_no is the authority
		$fields{'ref_is_authority_checked'} = 'checked';
		$fields{'ref_is_authority_notchecked'} = '';
	
	} else {
		# reference_no is not the authority
		
		if ((!$isNewEntry) || $secondTime) {
			# we only want to check a reference radio button
			# if they're editing an old record.  This will force them to choose
			# one for a new record.  However, if it's the second time
			# through, then it's okay to check one since they already did.
			$fields{'ref_is_authority_checked'} = '';
			$fields{'ref_is_authority_notchecked'} = 'checked';
		}
		
		if (!$secondTime) {
			$fields{'2nd_pages'} = $fields{'pages'};
			$fields{'2nd_figures'} = $fields{'figures'};
			$fields{'pages'} = '';
			$fields{'figures'} = '';
		}
	}
	
		
	
	# if the rank is species, then display the type_specimen input
	# field.  Otherwise display the type_taxon_name field.
	my $rankFromSpaces = Validation::taxonRank($self->{taxonName});
	
	# note, if secondTime through, then this is still the database rank,
	# but it was just pulled from the database on the first time through
	# and then passed in the $q object.
	my $databaseRank = $fields{taxon_rank};
	
	if ($rankFromSpaces eq 'species' || $rankFromSpaces eq 'subspecies') {
		# remove the type_taxon_name field.
		$fields{'OPTIONAL_type_taxon_name'} = 0;
	} else {
		# remove the type_specimen field.	
		$fields{'OPTIONAL_type_specimen'} = 0;
	}
	
	# Now we need to deal with the taxon rank popup menu.
	# if this authority record already existed, then use the taxon_rank
	# from the database.  Otherwise, use the rank obtained from the 
	# spacing of the name
	my $rankToUse = ($databaseRank || $rankFromSpaces);
	if ($rankToUse eq 'higher') {
		# the popup menu doesn't have "higher", so use "genus" instead.
		$rankToUse = 'genus';
	}
	
	$fields{taxon_rank} = $hbo->rankPopupMenu($rankToUse);
	

	# if the authorizer of this record doesn't match the current
	# authorizer, and if this is an edit (not a first entry),
	# then only let them edit empty fields.
	#
	# otherwise, they can edit any field.
	my @nonEditables;
	
	if ((! $isNewEntry) && ($s->get('authorizer_no') != $fields{authorizer_no})) {
	
		print "<p align=center><i>You did not create this record, so you can only edit empty fields.</i></p>";
		
		# we should always make the ref_is_authority radio buttons disabled
		# because only the original authorizer can edit these.
		
		push (@nonEditables, "ref_is_authority");
		
		# depending on the status of the ref_is_authority radio, we should
		# make the other reference fields non-editable.
		if ($fields{'ref_is_authority'} eq 'YES') {
			push (@nonEditables, ('author1init', 'author1last', 'author2init', 'author2last', 'otherauthors', 'pubyr', '2nd_pages', '2nd_figures'));
		} else {
			push (@nonEditables, ('pages', 'figures'));		
		}
		
		
		my @keys = keys(%fields);
		
		# find all fields which are not empty and add them to the list.
		foreach my $f (@keys) {
			if (($fields{$f} ne '') || ($fields{$f} != 0) ) {
				push(@nonEditables, $f);
			}
		}		
	}
	
	# print the form	
	print main::stdIncludes("std_page_top");
	print $hbo->newPopulateHTML("add_enter_authority", \%fields, \@nonEditables);
	print main::stdIncludes("std_page_bottom");
}




# Call this when you want to submit an authority form.
# Pass it the HTMLBuilder object, $hbo, the cgi parameters, $q, and the session, $s.
#
# The majority of this method deals with validation of the input to make
# sure the user didn't screw up, and to display an appropriate error message if they did.
#
# rjp, 3/2004.
sub submitAuthorityForm {
	my Taxon $self = shift;
	my $hbo = shift;
	my $s = shift;		# the cgi parameters
	my $q = shift;		# session

	my $sql = $self->getSQLBuilder();
	my $errors = Errors->new();
	
	# if this is the second time they submitted the form (or third, fourth, etc.),
	# then this variable will be true.  This would happen if they had errors
	# the first time, and then resubmitted it.
	my $isSecondSubmission = $q->param('second_submission');
	
	# is this a new entry, or an edit of an old record?
	my $isNewEntry = 1;  
	if ($self->{taxonNumber}) {
		$isNewEntry = 0;  # it must exist if we have a taxon_no for it.
	}

	# grab all the current data from the database about this record
	# if it's not a new entry (ie, if it already existed).	
	my %dbFields;
	if (! $isNewEntry) {
		my $results = $self->databaseRecord();

		if ($results) {
			%dbFields = %$results;
		}
	}
		
	# this $editAny variable is true if they can edit any field,
	# false if they can't.
	my $editAny = 0;
	
	if ($isNewEntry) {
		$editAny = 1;	# new entries can edit any field.
	} else {
		# edits of pre-existing records have more restrictions. 
	
		# if the authorizer of the authority record doesn't match the current
		# authorizer, then *only* let them edit empty fields.
	
		my $editAny = 0;
	
		# if the authorizer of the authority record matches the authorizer
		# who is currently trying to edit this data, then allow them to change
		# any field.
		if ($s->get('authorizer_no') == $dbFields{authorizer_no}) {
			$editAny = 1;	
		}
		
		if ($s->isSuperUser()) {
			# super user can edit any field no matter what.
			$editAny = 1;	
		}
	}
	
	
	# build up a hash of fields/values to enter into the database
	my %fieldsToEnter;
	
	if ($isNewEntry) {
		$fieldsToEnter{authorizer_no} = $s->authorizerNumber();
		$fieldsToEnter{enterer_no} = $s->entererNumber();
		$fieldsToEnter{reference_no} = $s->currentReference();
	} else {
		$fieldsToEnter{modifier_no} = $s->entererNumber();	
	}
	
	
	if (	($q->param('ref_is_authority') ne 'YES') && 
			($q->param('ref_is_authority') ne 'NO')) {
		#Debug::dbPrint("radio error");
		$errors->add("You must choose one of the reference radio buttons.");
	}
	
	# merge the pages and 2nd_pages, figures and 2nd_figures fields together
	# since they are one field in the database.
	
	if ($q->param('ref_is_authority') eq 'NO') {
		$fieldsToEnter{pages} = $q->param('2nd_pages');
		$fieldsToEnter{figures} = $q->param('2nd_figures');
		
		if (! $q->param('author1last')) {
			$errors->add('You must enter an author.');	
		}
	}
	
	
	if (($q->param('otherauthors')) && (! $q->param('author2last') )) {
		# don't let them enter other authors if the second author field
		# isn't filled in.
		
		$errors->add("Don't enter other authors if you haven't entered a second author.");
	}
	
	{ # so we have our own scope for these variables.
		my $rankFromSpaces = Validation::taxonRank($q->param('taxon_name'));
		my $trank = $q->param('taxon_rank');
		my $er = 0;
		
		if ( (($rankFromSpaces eq 'subspecies') && ($trank ne 'subspecies')) ||
			(($rankFromSpaces eq 'species') && ($trank ne 'species')) ||
			(($rankFromSpaces eq 'higher') && (
					($trank eq 'subspecies') || 
					($trank eq 'species')
					
					)) ) {

			$errors->add("The original rank, '" . $trank . "' doesn't match the spacing of the taxon name, '" . $q->param('taxon_name_corrected') . "'.");		
		}
	}
	
	# Now loop through all fields submitted from the form.
	# If a field is not empty, then see if we're allowed to edit it in the database.
	# If we can edit it, then make sure the name is correct (since a few fields like
	# 2nd_pages, etc. don't match the database field names) and add it to the 
	# %fieldsToEnter hash.
	
	foreach my $formField ($q->param()) {
		if (! $q->param($formField)) {
			next;  # don't worry about empty fields.	
		}
		
		my $okayToEdit = $editAny;
		if (! $okayToEdit) {
			# then we should do some more checks, because maybe they are allowed
			# to edit it aferall..  If the field they want to edit is empty in the
			# database, then they can edit it.
			
			if (! $dbFields{$formField}) {
				# If the field in the database is empty, then it's okay
				# to edit it.
				$okayToEdit = 1;
			}
		}
		
		if ($okayToEdit) {
			$fieldsToEnter{$formField} = $q->param($formField);
		}
		
		#Debug::dbPrint("okayToEdit = $okayToEdit, $formField = " . $q->param($formField));
		
	} # end foreach formField.
	
	if ($q->param('type_taxon_name')) {
		my $junk;
		($fieldsToEnter{type_taxon_no}, $junk) = $self->getTaxonNumberFromName($fieldsToEnter{type_taxon_name});
		$fieldsToEnter{type_taxon_name} = '';
	}

	$fieldsToEnter{taxon_name} = $q->param('taxon_name_corrected');	

	# delete some fields that may be present since these don't correspond
	# to fields in the database table..
	delete $fieldsToEnter{action};
	delete $fieldsToEnter{'2nd_authors'};
	delete $fieldsToEnter{'2nd_figures'};
	
	# correct the ref_is_authority field.  In the HTML form, it can be "YES" or "NO"
	# but in the database, it should be "YES" or "" (empty).
	if ($fieldsToEnter{ref_is_authority} eq 'NO') {
		$fieldsToEnter{ref_is_authority} = '';
	}
	
	
	# at this point, we should have a nice hash array (%fieldsToEnter) of
	# fields and values to enter into the authorities table.
	if ($isNewEntry) {
		# if it's a new entry, then insert it into the database.
		
		# *** NOTE, if they try to enter a new record which has the same name and
		# taxon_rank as an existing record, we should display a warning page stating
		# this fact..  Put all of the fields they're trying to enter in hidden fields
		# on this page.  Then allow them to *really* submit it if they want to.
		#
		# Could do this by having a separate method in bridge called 
		# submitDuplicateAuthorityForm, and that could call this 
		# submitAuthorityForm method and pass it an argument so we know that we 
		# shouldn't display the warning form *again*, but should actually enter it.
		
		$fieldsToEnter{created} = now();
		
		if (($self->getTaxonNumberFromName($fieldsToEnter{taxon_name})) && 
			(! $q->param('second_submission')) ) {
			
			# put a message in a hidden to let us know that the user *really*
			# wants to submit this duplicate record.  This way, when they click
			# the submit button the second time around, we won't display the 
			# same warning message again.
			
			$errors->add("The taxon '" . $fieldsToEnter{taxon_name} . "' already exists in our database. Are you sure you want to submit this record?");
		}
		
		if ($errors->count() > 0) {
			
			$q->param(-name=>'second_submission', -values=>['1']);
			
			# stick the errors in the CGI object for display.
			my $message = $errors->errorMessage();
						
			$q->param(-name=>'error_message', -values=>[$message]);
			
			
			$self->displayAuthorityForm($hbo, $s, $q);
			
			return;
		}
		
		$sql->insertNewRecord('authorities', \%fieldsToEnter);
		
		# now that we've inserted, we should grab the taxon_no
	}
	
	foreach my $fieldToEnter (keys(%fieldsToEnter)) {
		Debug::dbPrint("$fieldToEnter = " . $fieldsToEnter{$fieldToEnter});
	}
}




# pass this an HTMLBuilder object
# 
# display the form which allows users to enter/edit opinion
# table data.
#
sub displayOpinionForm {
	my Taxon $self = shift;
	my $hbo = shift;

	my %fields;
	my @nonEditables;
	
	print $hbo->newPopulateHTML("add_enter_opinion", \%fields, \@nonEditables);
}


















# end of Taxon.pm

1;