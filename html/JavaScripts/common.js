//some common java script functions which are used throughout the web site.
//written by ryan, 1/2004.

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
