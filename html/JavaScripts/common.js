//some common java script functions which are used throughout the web site.
//**note, these are for use on BOTH the PUBLIC and PRIVATE page.
//created by rjp, 1/2004.


// pass this a taxon name as a string,
// and it will look at the number of spaces to determine
// the rank.
//
// 0 spaces = higher
// 1 space  = species
// 2 spaces = subspecies
//
// it will return a string, either "higher", "species", or "subspecies"
// ** note, we can't tell the difference between a genus and a higher taxon
// by just looking at the spacing.. so a genus name will return as "higher" as well. 
function taxonRank(taxon) {
	var hasOneSpace = /.+[ ]{1}.+/;
	var hasTwoSpaces = /.+[ ]{2}.+/;
	
	if (hasTwoSpaces.test(taxon)) {
		return "subspecies";	
	}
	
	if (hasOneSpace.test(taxon)) {
		return "species";	
	}

	return "higher";
}


//show a popup window with the passed URL.
function tipsPopup (URL) {
	window.open(URL, 'tips', 'toolbar=1,scrollbars=1,location=1,statusbar=0,menubar=0,resizable=1,width=640,height=480');
}


//if java script is turned on, then this will
//print a hidden input field so we can check from the perl
//code and see that it was turned on.
function javascriptOnCheck() {
	document.writeln("<INPUT type=\"hidden\" name=\"javascripton\" value=\"yes\"></INPUT>");	
}


//meant to fill the years in a pull down menu from 1998 to the current year.
function fillYears() {
	var yearField = document.forms[0].year
	var date = new Date();

	var maxYear = date.getFullYear(); 
	var year;

	for (year = maxYear; year >= 1998; year--) {
		document.writeln("<OPTION>" + year + "</OPTION>");
	}	
}


function fillMonths() {
	document.write("<option>December</option>");
	document.write("<option>November</option>");
	document.write("<option>October</option>");
	document.write("<option>September</option>");
	document.write("<option>August</option>");
	document.write("<option>July</option>");
	document.write("<option>June</option>");
	document.write("<option>May</option>");
	document.write("<option>April</option>");
	document.write("<option>March</option>");
	document.write("<option>February</option>");
	document.write("<option>January</option>");
}

function fillDays() {
	var i;
	for (i = 31; i > 0; i--) {
		document.writeln("<OPTION>" + i + "</OPTION>");
	}
}
