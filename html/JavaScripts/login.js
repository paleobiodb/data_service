// 2/2004 by rjp.

// complete the authorizer field in the form.
function doCompleteAuthorizer(e) {
		
    //return if they press delete.
    if (e.which == 8 || e.which == 127) {
        return true;
    }

    //names in alphabetical order
    var names = new Array("Aberhan, M.", "Alroy, J.", "Bambach, R.", "Behrensmeyer, K.", "Bishop, L.", "Bodenbender, B.", "Bonuso, N.", "Bottjer, D.", "Boyce, C.", "Brett, C.", "Brochu, C.", "Budd, N.", "Carrano, M.", "Churchill-Dickson, L.", "Clyde, W.", "Croft, D.", "Danelian, T.", "DiMichele, B.", "Eble, G.", "Erwin, D.", "Fara, E.", "Foote, M.", "Fortelius, M.", "Fursich, F.", "Gastaldo, R.", "Gensel, P.", "Hansen, T.", "Harper, D.", "Head, J.", "Holland, S.", "Hotton, C.", "Huber, B.", "Hughes, N.", "Hunter, J.", "Ivany, L.", "Jablonski, D.", "Jacobs, D.", "Johnson, K.", "Kidwell, S.", "Kiessling, W.", "Knoll, A.", "Korn, D.", "Kosnik, M.", "Kowalewski, M.", "Kowalski, E.", "Labandeira, C.", "Lidgard, S.", "Looy, C.", "Lupia, R.", "Maples, C.", "Marshall, C.", "McKinney, M.", "Meng, J.", "Meyer, D.", "Miller, A.", "Niklas, K.", "O'Keefe, R.", "Oakley, T.", "Olszewski, T.", "Patzkowsky, M.", "Pfefferkorn, H.", "Plotnick, R.", "Raup, D.", "Raymond, A.", "Rees, A.", "Rogers, R.", "Roy, K.", "Royer, D.", "Schneider, J.", "Sepkoski, J.", "Sidor, C.", "Sims, H.", "Smith, D.", "Stein, W.", "Tiffney, B.", "Turner, A.", "Uhen, M.", "Villier, L.", "Visscher, H.", "Wagner, P.", "Wang, H.", "Wang, X.", "Werdelin, L.", "Wilf, P.", "Wing, S.");

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
function doCompleteEnterer(e) {

    //return if they press delete.
    if (e.which == 8 || e.which == 127) {
        return true;
    }

    //names in alphabetical order
    var names = new Array("Aberhan, M.", "Adam, P.", "Allen, J.", "Alroy, J.", "Apel, M.", "Arakaki, M.", "Bambach, R.", "Bean, J.", "Behrensmeyer, K.", "Bernard, C.", "Bezusko-Layou, K.", "Bishop, L.", "Bobe, R.", "Bodenbender, B.", "Bonuso, N.", "Borkow, P.", "Bottjer, D.", "Boyce, C.", "Boyle, N.", "Brenneis, B.", "Brett, C.", "Brochu, C.", "Broughton, J.", "Budd, N.", "Carlson, D.", "Carrano, M.", "Cassara, J.", "Childs, E.", "Churchill-Dickson, L.", "Clapham, M.", "Clyde, W.", "Cote, S.", "Croft, D.", "Danelian, T.", "DeLong, S.", "DiMichele, B.", "Eble, G.", "Eichiner, H.", "English, L.", "Erwin, D.", "Fara, E.", "Ferguson, C.", "Finarelli, J.", "Foote, M.", "Fortelius, M.", "Fosdick, J.", "Fursich, F.", "Gahr, M.", "Gannon, L.", "Gastaldo, R.", "Gensel, P.", "Gibson, M.", "Glavich, C.", "Hall, M.", "Hansen, T.", "Hanson, T.", "Harper, D.", "Head, J.", "Heim, N.", "Hempfling, D.", "Hendy, A.", "Hicks, S.", "Holland, S.", "Hotton, C.", "Huber, B.", "Hughes, N.", "Hunter, J.", "Ivany, L.", "Jablonski, D.", "Jacobs, D.", "Jacobson, J.", "Jamet, C.", "Johnson, K.", "Kidd, A.", "Kidwell, S.", "Kiessling, W.", "Kinchloe, A.", "Knoll, A.", "Korn, D.", "Kosnik, M.", "Kotyk, M.", "Koverman, K.", "Kowalewski, M.", "Kowalski, E.", "Krause, R.", "Krug, Z.", "Kuemmell, S.", "L.-H. Liow", "Labandeira, C.", "Leckey, E.", "Lester, P.", "Lidgard, S.", "Link, E.", "Looy, C.", "Loring, H.", "Low, S.", "Lupia, R.", "Maples, C.", "Marshall, C.", "McGowan, A.", "McKinney, M.", "Medved, M.", "Meng, J.", "Metz, C.", "Meyer, D.", "Miller, A.", "Moore, E.", "Naeher, T.", "Nell, K.", "Niklas, K.", "Novack-Gottshall, P.", "Nurnberg, S.", "O'Keefe, R.", "O'Regan, H.", "Oakley, T.", "Olszewski, T.", "Patzkowsky, M.", "Pendleton, S.", "Peters, S.", "Pfefferkorn, H.", "Plotnick, R.", "Puijk, W.", "Raup, D.", "Raymond, A.", "Reed, D.", "Rees, A.", "Reynolds, M.", "Rogers, R.", "Roy, K.", "Royer, D.", "Schneider, J.", "Sepkoski, J.", "Sessa, J.", "Sherwood, M.", "Sidor, C.", "Simpson, C.", "Sims, H.", "Smith, D.", "Smith, G.", "Sosa, E.", "Spencer-Lee, A.", "Stein, W.", "Straub, K.", "Sunderlin, D.", "Tiffney, B.", "Tomasovych, A.", "Tracy, K.", "Tuell, A.", "Turner, A.", "Uhen, M.", "Valiulis, E.", "Vaughn, R.", "Verbruggen, F.", "Villier, L.", "Viranta, S.", "Visaggi, C.", "Visscher, H.", "Wagner, P.", "Wang, H.", "Wang, X.", "Watters, J.", "Webber, A.", "Werdelin, L.", "Wertheim, J.", "Whatley, R.", "Wilborn, B.", "Wilf, P.", "Wing, S.");
    
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
        var match = new RegExp(names[i], "i");
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
    var match = new RegExp(input, "i");
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