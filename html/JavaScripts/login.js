// 2/2004 by rjp.

// complete the authorizer field in the form.
// must also pass it an array refernece to
// a list of names in alphabetical order
function doComplete(e, form_elem, names, skip_check_if_space) {
	
    //return if they press delete.
    if (e.which == 8 || e.which == 127) {
        return true;
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
    } else if (numMatches == 0) {
        var val = form_elem.value;
        var sub = val.substr(0, val.length -1);
        
        //check to make sure that they haven't already typed in a valid
        //name..  If they have, then don't let 
        //them type in any extra characters
        var lastChar = val.substr(val.length-1,1);
        // Only skip the check if the last char is a space (only relevant so ppl can enter
        // 10 my bin terms in interval form
        if (!(lastChar == " " && skip_check_if_space)) {
            if (alreadyPresent(sub, names)) {
                form_elem.value = sub;
                return;
            }
        }
    }
}

// pass it a name and an array of names
// returns true if the name exactly matches one of the elements
// in the array of names
function alreadyPresent(input, names) {
    var i;
    var len = names.length;

    var numMatches = 0;
    for (i = 0; i < len; i++) {
        var match = new RegExp("^" + input , "i"); //i means ignore case
        if (match.test(names[i])) {
            numMatches += 1;
        }
        //var match = new RegExp("^" + names[i] , "i"); //i means ignore case
        //if (match.test(input)) {
        //    numMatches += 1;
        //}
    }

    if (numMatches == 1) {
        return true;
    } else {
        return false;
    }
}



