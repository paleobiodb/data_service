// 2/2004 by rjp.

// complete the authorizer field in the form.
// must also pass it an array refernece to
// a list of names in alphabetical order
function doCompleteAuthorizer(e, names) {
	
    //return if they press delete.
    if (e.which == 8 || e.which == 127) {
        return true;
    }

    var val = document.forms[0].authorizer.value;
    var sub = val.substr(0, val.length -1);
    
    //check to make sure that they haven't already typed in a valid
    //name..  If they have, then don't let 
    //them type in any extra characters
    if (alreadyPresent(sub, names)) {
        document.forms[0].authorizer.value = sub;
        return;
    }
    
    //see if the partial name they've typed in matches.    
    var match = doComplete(document.forms[0].authorizer.value, names);
    
    if (match) {
        document.forms[0].authorizer.value = match;
    }
}

// complete the enterer field in the form.
// must also pass it an array refernece to
// a list of names in alphabetical order
function doCompleteEnterer(e, names) {

    //return if they press delete.
    if (e.which == 8 || e.which == 127) {
        return true;
    }

	    
    var val = document.forms[0].enterer.value;
    var sub = val.substr(0, val.length -1);
    
    //check to make sure that they haven't already typed in a valid
    //name..  If they have, then don't let 
    //them type in any extra characters
    if (alreadyPresent(sub, names)) {
        document.forms[0].enterer.value = sub;
        return;
    }
    
    //see if the partial name they've typed in matches.
    var match = doComplete(val, names);
    
    if (match) {
        document.forms[0].enterer.value = match;
    }
}

// pass it a name and an array of names
// returns true if the name exactly matches one of the elements
// in the array of names
function alreadyPresent(input, names) {
    var i;
    var len = names.length;
    
    for (i = 0; i < len; i++) {
        var match = new RegExp("^" + names[i] + "$", "i"); //i means ignore case
        if (match.test(input)) {
            return true;
        }
    }

    return false;
}


// pass it a value to match on, and an array of possible matches
function doComplete(input, names) {
    if (input == "") {
        return "";
    }
    
    var len = input.length;
    
    var numNames = names.length;
        
    // loop through each element in the names list
    var match = new RegExp("^" + input, "i");
    var lastMatch = "";
    
    var numMatches = 0;
    var i;
    for (i = 0; i < numNames; i++) {
        if (match.test(names[i])) {
            numMatches++;
            lastMatch = names[i];
        }
    }
    
    if (numMatches == 1) {
        return lastMatch;
    }
    
    return ""; //no match found            
}


