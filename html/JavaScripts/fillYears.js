//meant to fill the years in a pull down menu from 1998 to the current year.


	var yearField = document.forms[0].year
	var date = new Date();

	var maxYear = date.getFullYear(); 
	var year;

	for (year = maxYear; year >= 1998; year--) {
		document.write("<option>" + year + "</option>");
	}
