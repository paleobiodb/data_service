/*  CheckTaxonomyForm.js  

Used to perform sanity checking on Taxonomy Entry forms.	
Note, makes use of a couple of functions from the common.js file.

Rewritten by rjp, 2/2004  

*/


// check the form to make sure it's legal to submit
function checkForm() {
	var frm = document.forms[0];
	var errors = new Error();

	var rank = "";
	var originalRank = "";
	
	if (frm.taxon_name_corrected) {
		// it's possible that the taxon_name_corrected field won't exist 
		// for example, if the user has already entered a species where the genus
		// didn't already exist, then it will prompt them to enter authority data
		// on the genus.  However, the taxon_name_corrected field isn't in this form.
	
		// figure out the rank of the taxon name.	
		rank = taxonRank(frm.taxon_name_corrected.value);
		originalRank = taxonRank(frm.taxon_name.value);

		//alert("original = " + originalRank + " corrected = " + rank);
	
		if (rank == "invalid") {
			errors.add("The taxon being edited is ill-formed.");
		}
		
		// make sure that the rank they started with in taxon_name is the
		// same as the edited rank.  This is the only check we have to do with the
		// original taxon name.
		if (rank != originalRank) {
			errors.add("The taxon's rank in the top field can't be changed.");
		}
		
		
	}
			
	
	// if the user enters a new taxon which is a species and the genus has not been 
	// entered, they will be taken to a form which has the same fields, but with
	// parent_ prepended on each one.
	// 
	// so we have to check for this.
	//
	if (frm.no_authority_data) {  
		// no_authority_data is inserted by a perl script if the parent taxon
		// doesn't have any authority data in the database.
		originalRank = taxonRank(frm.parent_taxon_name.value);
		
		// check the authority reference info..
		errors.appendErrors(checkReferenceInfo(frm.parent_ref_is_authority.checked, "authority", frm.parent_pages.value, frm.parent_figures.value, frm.parent_author1init.value, frm.parent_author1last.value, frm.parent_author2init.value, frm.parent_author2last.value, frm.parent_otherauthors.value, frm.parent_pubyr.value, frm.elements['parent_2nd_pages'].value, frm.elements['parent_2nd_figures'].value));
		
		if (!(specificRankMatchesGeneralRank(originalRank, frm.parent_taxon_rank.value))) {
			errors.add("Original rank is invalid.");
		}
		
	} else {
	
		// check the authority reference info..
		errors.appendErrors(checkReferenceInfo(frm.ref_is_authority.checked, "authority", frm.pages.value, frm.figures.value, frm.author1init.value, frm.author1last.value, frm.author2init.value, frm.author2last.value, frm.otherauthors.value, frm.pubyr.value, frm.elements['2nd_pages'].value, frm.elements['2nd_figures'].value));
		
		if (!(specificRankMatchesGeneralRank(rank, frm.taxon_rank.value))) {
			errors.add("Original rank is invalid.");
		}
	}
	
	if (frm.opinion_present) {
		// Check taxonomic opinion data if any are present on the page
		errors.appendErrors(checkOpinion());
	}
	
	// Report errors
	if (errors.count() > 0) {
		errors.showAlert()
		return false;
	} 
		
	return true;
}




// check the opinion portion of the form
function checkOpinion() {
	var frm = document.forms[0];
	var errors = new Error();
		
	// figure out the rank of the taxon name.	
	var rank = taxonRank(frm.taxon_name_corrected.value);
		
	var checkedRadio = "";
	for (var i = 0; i < frm.taxon_status.length; i++) {
		if (frm.taxon_status[i].checked) {
			checkedRadio = frm.taxon_status[i].value;
		}
	}	
	

	
	//********************************
	// check the fields at the bottom of the form (the radio button ones)
	
	if (!(checkedRadio == "no_opinion")) {
		// if they're entering opinion data, then we need to check the
		// opinion reference section.
	
	
		// check the opinion reference info..
		errors.appendErrors(checkReferenceInfo(frm.ref_has_opinion.checked, "opinion", frm.opinion_pages.value, frm.opinion_figures.value, frm.opinion_author1init.value, frm.opinion_author1last.value, frm.opinion_author2init.value, frm.opinion_author2last.value, frm.opinion_otherauthors.value, frm.opinion_pubyr.value, frm.elements['opinion_2nd_pages'].value, frm.elements['opinion_2nd_figures'].value));

	}
	
	
	switch (checkedRadio) {
	
	case "no_opinion": 
	// ***** no_opinion ***** 
	
		break;
	
	// ***** END OF no_opinion *****
	
	case "belongs_to":	
	// ***** belongs_to ***** 		
		
		
		break;
		
	// ***** END OF belongs_to ***** 	
	
	case "recombined_as":
	// ***** recombined_as ***** 
		if (frm.parent_taxon_name.value == "") {
			if ( taxonRank(frm.taxon_name_corrected) == "higher" )	{
				errors.add("Higher taxon name is missing.");
			} else	{
				errors.add("Name of combination is missing.");
			}
			
			break;
		}
	
		if (frm.taxon_name.value == frm.parent_taxon_name.value)	{
			errors.add("Recombined field can't have the same name as the original taxon.");
		}
	
		if (taxonRank(frm.parent_taxon_name.value) != rank) {
			errors.add("Recombined rank doesn't match original rank.");
		}
	
		if (frm.type_taxon_name.value == frm.parent_taxon_name.value)	{
			errors.add("Type taxon and higher taxon have the same name.");
		}
		
		if (taxonRank(frm.parent_taxon_name.value) == "invalid") {
			errors.add("'recombined as field' is ill-formed.");
		}
	
	
		break;
	// ***** END OF recombined_as ***** 
	
	case "invalid1":
	// ***** invalid1 ***** 
	
		var field = frm.parent_taxon_name2.value;
		
		if (field == "") {
			errors.add("The '" + frm.synonym.value + "' field is empty.");
		}
	
		if (taxonRank(frm.parent_taxon_name2.value) != rank) {
			errors.add("The '" + frm.synonym.value + "' rank doesn't match original rank.");
		}
	
		if (field == frm.taxon_name_corrected.value) {
			errors.add("The '" + frm.synonym.value + "' field matches the taxon being edited.");
		}
	
		if (taxonRank(frm.parent_taxon_name2.value) == "invalid") {
			errors.add("'" + frm.synonym.value + "' is ill-formed.");
		}
	
		break;
		
	// ***** END OF invalid1 *****
	
	case "invalid2":
	// ***** invalid2 ***** 
		break;
		
	// ***** END OF invalid2 ***** 

	}
	
	return errors;
}


// used to check the reference section for both authorities and opinions
// returns an error object if any errors are found.
//
// the first argument is a boolean, true if the "current reference" check box is 
// checked, false otherwise.  The other arguments are strings which correspond
// to the fields in the reference form.
//
// type is either "authority" or "opinion"
//
function checkReferenceInfo(checked, type, currentPages, currentFigures, firstAuthorInitial, firstAuthorLast, secondAuthorInitial, secondAuthorLast, otherAuthors, year, pages, figures) {

	var errors = new Error();
	
	var refsAreBlank = ((firstAuthorInitial == "") &&
		(firstAuthorLast == "") &&
		(secondAuthorInitial == "") &&
		(secondAuthorLast == "") &&
		(year == "") &&
		(otherAuthors == "") &&
		(pages == "") &&
		(figures == ""));
			
	// First, make sure that they either selected the 
	// use current reference check box
	// or fill in some information in the reference fields.
	if (checked) {
		// then make sure that the fields are blank
		
		if (! refsAreBlank) {
			errors.add("Don't fill in " + type + " reference fields if the 'use current reference' checkbox is checked.");		
		}
		
	} else {  // the checkbox is not checked
		// so we need to check the validity of the reference information.
		if (refsAreBlank) {
			errors.add("You must fill in an " + type + " reference or check the 'use current reference' checkbox.");
		} else {
			//the refs aren't blank
			
			//at a minimum, we need the author last name and pubyr.
			if (!(properLastName(firstAuthorLast))) {
				errors.add("Ill-formed or missing " + type + " last name.");
			}
			if (year && !(properYear(year))) {
				errors.add("Ill-formed " + type + " year.");
			}
		
			if (firstAuthorInitial != "") {
				if (! properInitial(firstAuthorInitial)) {
					errors.add("Ill-formed " + type + " initial");
				}
			}
			
			if (secondAuthorLast != "") {
				if (! properInitial(secondAuthorLast)) {
					errors.add("Ill-formed " + type + " last name");
				}
			}
			
			if (secondAuthorInitial != "") {
				if (! properInitial(secondAuthorInitial)) {
					errors.add("Ill-formed " + type + " initial");
				}
			}
		}
	}
	
	return errors;

}



//end of CheckTaxonomyForm.js
