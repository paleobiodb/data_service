// 2/2004 by rjp.

// complete the authorizer field in the form.
// must also pass it an array refernece to
// a list of names in alphabetical order
function doComplete(e, form_elem, names, skip_already_present_check) {
	
    //return if they press delete.
    if (e.which == 8 || e.which == 127) {
        return true;
    }

    var val = form_elem.value;
    var sub = val.substr(0, val.length -1);
    
    //check to make sure that they haven't already typed in a valid
    //name..  If they have, then don't let 
    //them type in any extra characters
    if (!skip_already_present_check) {
        if (alreadyPresent(sub, names)) {
            form_elem.value = sub;
            return;
        }
    }
    
    // loop through each element in the names list
    var match = new RegExp("^" + form_elem.value, "i");
    
    var lastMatch = "";
    var numMatches = 0;
    for (var i = 0; i < names.length; i++) {
        if (match.test(names[i])) {
            numMatches++;
            lastMatch = names[i];
        }
    }
    
    if (numMatches == 1) {
        form_elem.value =lastMatch;
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



