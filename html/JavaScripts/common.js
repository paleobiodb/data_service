//some common java script functions which are used throughout the web site.
//note, these are for use on BOTH the PUBLIC and PRIVATE page.
//created by rjp, 1/2004.


// To be run at startup to enable the css drop downs
// See http://www.alistapart.com/articles/dropdowns/
// Attaches :hover function to LI class for IE 5.01+, 
// normally IE doesn't support :hover property
// for things over than <A> tags
sfHover = function() {
    if (document.getElementById("dropDown")) {
        var sfEls1 = document.getElementById("dropDown").getElementsByTagName("TD");
        var sfEls2 = document.getElementById("dropDown").getElementsByTagName("LI");
        for (var i=0; i<sfEls1.length; i++) {
            sfEls1[i].onmouseover=function() {
                this.className+=" sfhover";
            }
            sfEls1[i].onmouseout=function() {
                this.className=this.className.replace(new RegExp(" sfhover\\b"), "");
            }
        }
        for (var i=0; i<sfEls2.length; i++) {
            sfEls2[i].onmouseover=function() {
                this.className+=" sfhover";
            }
            sfEls2[i].onmouseout=function() {
                this.className=this.className.replace(new RegExp(" sfhover\\b"), "");
            }
        }
    }
}
if (window.attachEvent) window.attachEvent("onload", sfHover);


// This is a handy little function for seeing what properties an object has
function showProperties ( obj ) {
    var result = "";

    for ( var i in obj ) {
        result += "obj." + i + " = " + obj[i] + "<BR>\n";
    }
    document.write ( result );
}  

function checkBrowserVersion() {
	var match = /Microsoft/i;
	
	if (match.test(navigator.appName)) {
		document.write("<div class=\"warning\">Warning: Internet Explorer is not standards compliant and you should not use it on this web site or elsewhere. <BR> Aside from cosmetic defects in page rendering, error checking routines may not work correctly and you may be randomly logged out of the site.<BR><BR> Please download and use <a href=\"http://www.apple.com/safari/\">Safari</a> or <a href=\"http://www.mozilla.org/firefox/\">Firefox</a>, or read the web browser <A HREF=\"javascript: tipsPopup\('/public/tips/browser_tips.html'\)\">tip sheet</A> for information on other free browsers.</div>");
	}
}


//
// for validation of form input
//

// returns true if the input is in a proper format for a last name
function properLastName(input) {
	if ((!input) || input == "") { return false; }
	var match = /^[A-Za-z,-.\'][A-Za-z ,-.\']*$/;
	return match.test(input);
}

// returns true if the input is in a proper format for an initial 
function properInitial(input) {
	if ((!input) || input == "") { return false; }
	var match1 = /^[A-Z][A-Za-z .]*$/;
	var match2 = /[.]/;
	return (match1.test(input) && match2.test(input));
}

// returns true if it's a proper year format which begins with a 1 or 2,
// ie, 1900 and something or 2000 something.
function properYear(input) {
	if ((!input) || input == "") { return false; }
	var match = /^[12]\d{3}$/;
	return match.test(input);
}

//
// by rjp, 2/2004, modified by PS
function guessTaxonRank(taxon) {
    var isSpecies = /^[A-Z][a-z]+[ ][a-z]+\n?$/;
    var isSubspecies = /^[A-Z][a-z]+[ ][a-z]+[ ][a-z]+\n?$/;
    var isHigher = /^[A-Z][a-z]+\n?$/;
    
    //alert ("taxon = '" + taxon + "'");
    if (isSubspecies.test(taxon)) {
        return "subspecies";    
    } else if (isSpecies.test(taxon)) {
        return "species";   
    } else if (isHigher.test(taxon)) {
        return "higher";
    } else {
        return "invalid";
    }
}



// Error is a JavaScript class for error reporting..  
// To use it, first create a new instance of the object, for example
// var err = new Error();  // you can optionally pass an error message in the constructor
// Then, call the add() method each time you want to add an error to it.
// err.add("my error message");
// When finished, you can print the alert by calling the showAlert method,
// or you can directly grab the string.
// err.showAlert();  //displays the alert message with the entire error
// alert(err);  // same thing

// constructor, optionally pass it the first error message.
//
// by rjp, 2/2004
function Error(msg) {
	this.starting = "\tPlease fix the following errors:\n\n";
	this.message = "";
	this.internalcount = 0;
	
	if (msg) {
		this.message = "* " + msg;	
	}	
	
	this.ending = "\n\nRefer to the tip sheet for instructions.";
}

// adds an error with a bullet point to the list of error messages
Error.prototype.add = function(msg) {
	this.message += "\n* " + msg;
	this.internalcount += 1;
}

Error.prototype.count = function() {
	return this.internalcount;
}

// displays an alert message with the error
Error.prototype.showAlert = function() {
	alert(this);
}

// converts the error object to a string.
Error.prototype.toString = function() {
	return this.starting + this.message + this.ending;
}

// pass this method another Error object, and it will append the
// new object onto the end of itself.
Error.prototype.appendErrors = function(newError) {
	this.message += newError.message;
	this.internalcount += newError.internalcount;
}

// End of error class.




//show a popup window with the passed URL.
function tipsPopup (URL,width,height) {
    var width  = (width == null) ? 640 : width;
    var height = (height== null) ? 480 : height;
	window.open(URL, 'tips', 'toolbar=1,scrollbars=1,location=1,statusbar=0,menubar=0,resizable=1,width='+width+',height='+height);
}

//show a popup window with the passed URL.
function imagePopup (URL,width,height) {
    var width  = (width == null) ? 800 : width;
    var height = (height== null) ? 600 : height;
	window.open(URL, 'imageview', 'toolbar=0,scrollbars=1,location=0,statusbar=0,menubar=0,resizable=1,width='+width+',height='+height);
}

// Show a popup with a map in it.  this is kept distinct from the other two popups because if we include
// the full url in the javascript, search engines will still spider it, which is bad since the map 
// generation is ultra heavyweight
function mapPopup(collection_no) {
    var link = 'bridge'+'.pl'+
               '?action=displayMapOfCollection'+
               '&display_header=NO'+
               '&collection_no='+collection_no;
    window.open(link,'mapview','toolbar=0,scrollbars=1,location=0,statusbar=0,menubar=0,resizable=1,width=800,height=600');
}


//if java script is turned on, then this will
//print a hidden input field so we can check from the perl
//code and see that it was turned on.
function javascriptOnCheck() {
	document.writeln("<INPUT type=\"hidden\" name=\"javascripton\" value=\"yes\"></INPUT>");	
}


//meant to fill the years in a pull down menu from 1998 to the current year.
//
// by rjp, 2/2004
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
	document.write("<option value=12>December</option>");
	document.write("<option value=11>November</option>");
	document.write("<option value=10>October</option>");
	document.write("<option value=9>September</option>");
	document.write("<option value=8>August</option>");
	document.write("<option value=7>July</option>");
	document.write("<option value=6>June</option>");
	document.write("<option value=5>May</option>");
	document.write("<option value=4>April</option>");
	document.write("<option value=3>March</option>");
	document.write("<option value=2>February</option>");
	document.write("<option value=1>January</option>");
}

function fillDays() {
	var i;
	for (i = 31; i > 0; i--) {
		document.writeln("<OPTION>" + i + "</OPTION>");
	}
}

// Email addresses pass through a simple javascript, which most automated scripts won't execute
// before being displayed
function descram(part2r,part1r) {
    var em = "";
    for (i = 0; i <= part2r.length; i++) {
      em = part2r.charAt(i) + em;
    } 
    em = "@" + em;
    for (i = 0; i <= part1r.length; i++) {
      em = part1r.charAt(i) + em;
    } 
    document.write(em);
}


//pass this function a checkbox.
function checkAll(checkbox,the_class){
    var state = checkbox.checked;
    var frm = document.forms[0];
    for(var i=0; i < frm.elements.length; i++){
        if(frm.elements[i].className == the_class) {
            frm.elements[i].checked = state;
        }
    }
}   

// used in the PAST module to hide or show lines of text that pop up next
//  to data points in plots on mouse over; assumes that the named elements
//  are single lines of text stacked on top of each other
function showHide(what)	{
        var elems = document.getElementsByName(what);
        for (var i=0,a; a=elems[i]; i++)	{
                if ( a.style.visibility != "hidden" )	{
                        a.style.visibility = "hidden";
                        a.style.height = "0em";
                } else	{
                        a.style.visibility = "visible";
                        a.style.height = "1.25em";
                }
        }
}

