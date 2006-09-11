// Initialize this wehn we actually need it
var collections = new Array();
var taxa_by_row = new Array();
var autofill_values = new Object();
var autofill_values_init = 0;
var cur_row_num;
var cur_collection_no;

function handleSubmit() {
    var f = document.forms[0];
    if (f.genus_name.value != "") {
        insertOccurrenceRow();
        return false;
    } else {
        return checkForm();
    }
}

function checkForm() {
    var table = document.getElementById('occurrencesTable');

    var f = document.forms[0];
    
    var abund_unit = f.abund_unit.options[f.abund_unit.selectedIndex].value;

    initTaxaArray();

    var isInteger = /^[0-9]+$/;
    var isNumeric = /^[0-9]*\.?[0-9]+$/;
    var isIntegerOrHalf= /^[0-9]+$|^[0-9]*(\.[05])?$/;
    var isPresense = /^[xX]$/;
    var isAbundElem = /^abund_value_/;
    var errors = "";
    var cellct = 0;
    for (var i = 0; i < f.elements.length; i++) {
        var el = f.elements[i];
        if (el.name && isAbundElem.test(el.name)) {
            var val = el.value;
            
            if (val != "" && !isPresense.test(val)) {
                var error = "";
                switch (abund_unit) {
                    case 'specimens':
                    case '# of quadrats':
                        if (!isInteger.test(val)) {
                            error = " must be an integer\n";
                        }
                        break;
                    case 'individuals':
                        if (!isIntegerOrHalf.test(val)) {
                            error = " must be a whole number or half (i.e 3 or 3.5)\n";
                        }
                        break;
                    case 'category':
                    case 'rank':
                    case '':
                        // Anything goes for these ones
                        break;
                    default:
                        if (!isNumeric.test(val)) {
                            error = " must be a number\n";
                        }
                }
                if (error != "") {
                    var bits = el.name.split("_");
                    var row_num = bits[2];
                    var collection = bits[3];
                    var taxon_bits = taxa_by_row[row_num].split("-_");
                    var taxon = taxon_bits[0];
                    if (taxon_bits[1] != "") {
                        taxon += " ("+taxon_bits[1]+")";
                    }
                    taxon += " "+taxon_bits[2];
                    errors += "Abundance for "+taxon+" in collection number "+collection+" "+error;
                }
            }
        }
    }

    if (errors) {
        alert("Errors:\n"+errors);
        return false;
    } else {
        return true;
    }
}

function initCollectionsArray() {
    if (collections.length == 0) {
        collection_divs = document.getElementsByName('collection_nos');
        for(var i=0;i<collection_divs.length;i++) {
            collections[i]=collection_divs[i].value;
        }
    }
}

function initTaxaArray(force_init) {
    if (force_init || taxa_by_row.length == 0) {
        var allRows = document.getElementsByName('row_num');
        // This block interates over all the rows and checks to make sure we're not duplicating anything
        for(var i=0;i<allRows.length;i++) {
            var row_num = allRows[i].value;
            var elem = eval('document.forms[0].taxon_key_'+row_num);
            var taxon_name = elem.value;
            taxa_by_row[row_num] = taxon_name;
        }
    }
}

function initAutoFillHash() {
    if (autofill_values_init == 0) {
        autofill_values_init = 1;
        var f = document.forms[0];
        var isNumeric = /^[0-9]*\.?[0-9]+$/;
        var isPresense = /^[xX]$/;
        var isAbundElem = /^abund_value_/;
        for (var i = 0; i < f.elements.length; i++) {
            var el = f.elements[i];
            if (el.name && isAbundElem.test(el.name) && 
                el.value != '' && el.value.length > 2 && 
                !isNumeric.test(el.value) && !isPresense.test(el.value)) {
                autofill_values[el.value] = 1;       
            }
        }
    }
}

function updateAutoFillHash(e) {
    if (!e) e = window.event;
    if (!e) return;

    var el;
    if (e.srcElement) el = e.srcElement
    else if (e.target) el = e.target;
    else return;

    if (el &&  autofill_values_init == 1) {
        var isNumeric = /^[0-9]*\.?[0-9]+$/;
        var isPresense = /^[xX]$/;
        var isAbundElem = /^abund_value_/;
        if (el.name && isAbundElem.test(el.name) && 
            el.value != '' && el.value.length > 2 && 
            !isNumeric.test(el.value) && !isPresense.test(el.value)) {
            autofill_values[el.value] = 1;       
        }
    }
}

function insertOccurrenceRow() {
    var table = document.getElementById('occurrencesTable');

    var allRows = document.getElementsByName('row_num');
    var row_count= allRows.length; 
    
    var row = table.insertRow(-1);
    initCollectionsArray();
    var f = document.forms[0];
    var genus_reso = f.genus_reso.value;
    var genus_name = f.genus_name.value;
    // Do object detection - may not exist
    var subgenus_reso = "";
    if (f.subgenus_reso) 
        subgenus_reso = f.subgenus_reso.value;
    var subgenus_name = "";
    if (f.subgenus_name) 
        subgenus_name = f.subgenus_name.value;
    var species_reso = f.species_reso.value;
    var species_name = f.species_name.value;
    var taxon_name = formatOccurrence(genus_reso,genus_name,subgenus_reso,subgenus_name,species_reso,species_name);
    var taxon_key = genus_name+"-_"+subgenus_name+"-_"
                 + species_name+"-_"+genus_reso+"-_"
                 + subgenus_reso +"-_"+species_reso;

    // This block interates over all the rows and checks to make sure we're not duplicating anything
    for(var i=0;i<allRows.length;i++) {
        var this_row = allRows[i].value;
        var elem = eval('document.forms[0].taxon_key_'+this_row);
        if (elem.value == taxon_key) {
            alert("Name is a duplicate");
            return false;
        }
    }

    // This block does more error checking and makes sure the Genus/Subgenus/species look valid
    var informal= new RegExp("informal"); 
    var validGenus = new RegExp("^[A-Z][a-z]+$"); 
    var validSpecies = new RegExp("^[a-z]+$|^sp.|^indet.");
    if (genus_name == "") { 
        alert("Genus name is empty");
        return false;
    } else if (species_name == "") {
        alert("Species name is empty");
        return false;
    } else if (!informal.test(genus_reso) && !validGenus.test(genus_name)) {
        alert("Bad genus name, check capitalization");
        return false;
    } else if (!informal.test(species_reso) && !validSpecies.test(species_name)) {
        alert("Bad species name, check capitalization");
        return false;
    } else if (subgenus_name != "") {
        if (!informal.test(subgenus_reso) && !validGenus.test(subgenus_name)) {
            alert("Bad subgenus name, check capitalization, and don't include ( or )");
            return false;
        }
    }

    var rowClass="";
    var inputClass="fixedInput";
    if (row_count % 2 == 0) {
        rowClass="darkList";
        inputClass="fixedInputDark";
    }
    var taxon_name_escaped = taxon_name.replace(/"/g,'&quot;');  //"
    for(var i=0;i<collections.length;i++) {
        var cell = row.insertCell(i); 
        var collection_no = collections[i];
        var html = '<div class="fixedColumn"><input id="abund_value_'+row_count+'_'+collection_no+'" name="abund_value_'+row_count+'_'+collection_no+'" size="4" value="" class="'+inputClass+'" \/></div>'
                 + '<input name="occurrence_no_'+row_count+'_'+collection_no+'" value="-1" type="hidden" \/>';
        cell.innerHTML = html;
        cell.onclick = function () { cellInfo(row_count,collection_no,f.reference_no.value,0,''); }
        cell.className="fixedColumn";

        if (cell.firstChild && cell.firstChild.firstChild) {
            var inputEl = cell.firstChild.firstChild;
            if (inputEl) {
                inputEl.onblur = updateAutoFillHash;
                inputEl.onkeypress = handleKeyPress;
            }
        }
    }
    var first_cell = row.insertCell(0);
    first_cell.className="fixedColumn";
    row.className = rowClass;
    var taxon_key_escaped = taxon_key.replace(/"/g,'&quot;'); //"
    first_cell.innerHTML = taxon_name 
                         + ' <input type="hidden" name="row_num" value="'+row_count+'" \/>'
                         + ' <input type="hidden" name="taxon_key_'+row_count+'" value="'+taxon_key_escaped+'" \/>';

    f.genus_name.value="";
    if (f.subgenus_name) 
        f.subgenus_name.value="";
    f.species_name.value="";
    initTaxaArray(true);
    return true;
}

// Swaps out static text in the cell with a hidden input for editing
function editCell(row_num,collection_no) {
    cur_row_num = row_num;
    cur_collection_no = collection_no;
    var abund_id = 'abund_value_'+row_num+'_'+collection_no;
    var inputEl = document.getElementById(abund_id);
//    if (!inputEl) 
//        alert("dummy id:"+dummy_id+" input id:"+input_id);
    if (inputEl.type=="hidden") {
        var dummy_id = 'dummy_'+row_num+'_'+collection_no;
        var dummyEl = document.getElementById(dummy_id); 
        var html = '<input type="text" id="'+abund_id+'" name="'+abund_id+'" value="'+inputEl.value+'" autocomplete="OFF" size="4" class="'+inputEl.className+'"';
        // red is edit only
        if (inputEl.style.color == 'red') {
            html += 'readonly style="color: red;"';
        }
        html += ' />';
        dummyEl.innerHTML=html;
       /* 
        var new_input = document.createElement('input');
        new_input.setAttribute('type','text');
        new_input.setAttribute('id',abund_id);
        new_input.setAttribute('name',abund_id);
        new_input.setAttribute('value',inputEl.value);
        new_input.setAttribute('class',inputEl.className);
        new_input.setAttribute('className',inputEl.className);
        new_input.style.color=inputEl.style.color;

        while (dummyEl.firstChild) {
            dummyEl.removeChild(dummyEl.firstChild);
        }
        dummyEl.appendChild(new_input);
        */
        inputEl = document.getElementById(abund_id);
        inputEl.onkeydown = handleKeyPress;
        inputEl.onblur = updateAutoFillHash;
    }

    // Don't call focus() directly! editCell is sometimes called form
    // within a onKeyDown event.  When an focus event is called form within
    // another event it seems to behave erratically, with mac firefox and safari
    // erroring out.  So ensure the focus event is called independently by the browser.
    setTimeout('doFocus()',50);
}

function doFocus () {
    document.getElementById('abund_value_'+cur_row_num+'_'+cur_collection_no).focus();
}

function doAutoFill(row_num,collection_no) {
    initAutoFillHash();
    var f = document.forms[0];
    var abund_unit = f.abund_unit.options[f.abund_unit.selectedIndex].value;
    if (abund_unit == 'category' || abund_unit == 'rank') {
        var el = document.getElementById('abund_value_'+row_num+'_'+collection_no);
        var val_ct = 0;
        if (el.value.length >= 1 && (new RegExp('^[a-zA-z]')).test(el.value)) {
            var match_count = 0;
            var last_match = "";
            var elMatch = new RegExp("^"+el.value);
            for(var m in autofill_values) {
                val_ct++;
                if (m != 1) {
                    if (elMatch.test(m) && el.value != m) {
                        match_count++
                        last_match = m;
                    }
                }
            }
//            alert("match count:"+match_count+" last:"+last_match);
            if (match_count == 1) {
                el.value=last_match;
            }
        }
    }
}

function handleKeyPress (e){
    if (!e) e = window.event;
    if (!e) return;

    initCollectionsArray();
    initTaxaArray();

    var code;
    if (e.keyCode) code = e.keyCode;
    else if (e.which) code = e.which;
    else return;

    var el;
    if (e.srcElement) el = e.srcElement
    else if (e.target) el = e.target;
    else return;
   
    var id_bits = el.id.split("_");
    var row_num = parseInt(id_bits[2]);
    var collection_no = id_bits[3];
    var col_idx;
    for(var j=0;j<collections.length;j++) {
        if (collection_no == collections[j]) {
            col_idx = j;
            break;
        }
    }
    
    if (e.shiftKey && code == 9) { //left - shift+tab
        if (col_idx >= 1) {
            col_idx--;
        } else {
            col_idx = collections.length - 1
            if (row_num < taxa_by_row.length >= 1) 
                row_num--;
        }
        collection_no = collections[col_idx];
        
        editCell(row_num,collection_no);
        return false;
    } else if (code == 38) { //up
        if (row_num >= 1)
            row_num--;

        editCell(row_num,collection_no);
        return false;
    } else if ( code == 9) { //right tab
        setTimeout('doAutoFill('+row_num+','+collection_no+')',100);
        if (col_idx < collections.length - 1 ||
            row_num < taxa_by_row.length - 1) {
            if (col_idx < collections.length - 1) {
                col_idx++;
            } else {
                col_idx = 0;
                if (row_num < taxa_by_row.length - 1) 
                    row_num++;
            }
            collection_no = collections[col_idx];

            editCell(row_num,collection_no);
            return false;
        } else {
            // this happens on the very last cell only - just tab normally
            // onto the "add new taxon" row
            return true;
        }
    } else if (code == 40) { //down
        if (row_num < taxa_by_row.length - 1) 
            row_num++;
        
        editCell(row_num,collection_no);
        return false;
    }
    return true;
}

function cellInfo(i,collection,reference,read_only,authorizer) {
    var cell_info = document.getElementById('cell_info');

    var taxon_key = eval('document.forms[0].taxon_key_'+i+'.value');
    var bits = taxon_key.split("-_");
    var taxon_name = formatOccurrence(bits[3],bits[0],bits[4],bits[1],bits[5],bits[2]);
    
    var abund_value = eval('document.forms[0].abund_value_'+i+'_'+collection+'.value');

    var html = "";
    html += '&nbsp;Taxon: '+taxon_name+"<br />";
    html += '&nbsp;Collection: <a target="_blank" href="bridge.pl?action=displayCollectionDetails&collection_no='+collection+'">'+collection+"<\/a><br />";
    html += '&nbsp;Reference: <a target="_blank" href="bridge.pl?action=displayReference&reference_no='+reference+'">'+reference+"<\/a><br />";
    if (authorizer != '') {
        if (read_only) {
            html += '&nbsp;<span class="red">Authorizer: '+authorizer+'<\/span><br />';
        } else {
            html += '&nbsp;Authorizer: '+authorizer+'<br />';
        }
    }
    if (abund_value.length > 4) {
        html += '&nbsp;Abundance: '+abund_value+'<br />';
    }
    cell_info.innerHTML=html;
}

function formatOccurrence(genus_reso,genus_name,subgenus_reso,subgenus_name,species_reso,species_name) {
    var taxon_name = "";
    var isSpecies = 1;
    if ((new RegExp("^indet")).test(species_name) || (new RegExp("informal")).test(genus_reso)) {
        isSpecies = 0;
    }
    
    if (isSpecies) {
        taxon_name += "<i>";
    }

    if (genus_reso == 'n. gen.') {
        taxon_name += genus_name+" n. gen.";
    } else if (genus_reso == '"') {
        taxon_name += '"'+genus_name;
        if (!(subgenus_reso == '"' || species_reso == '"')) {
            taxon_name += '"';
        }
    } else if (genus_reso) {
        taxon_name += genus_reso+" "+genus_name;
    } else {
        taxon_name += genus_name;
    }

    if (subgenus_name) {
        taxon_name += " (";
        if (subgenus_reso == 'n. subgen.') {
            taxon_name += subgenus_name+" n. subgen.";
        } else if (subgenus_reso == '"') {
            if (genus_reso != '"') {
                taxon_name += '"';
            }
            taxon_name += subgenus_name;
            if (species_reso != '"') {
                taxon_name += '"';
            }
        } else if (subgenus_reso) {
            taxon_name += subgenus_reso+" "+subgenus_name;
        } else {
            taxon_name += subgenus_name;
        }
        taxon_name += ")";
    }

    taxon_name += " ";
    if (species_reso == '"') {
        if (!(genus_reso == '"' || subgenus_reso == '"')) {
            taxon_name += '"';
        }
        taxon_name += species_name+'"';
    } else if (species_reso && species_reso != 'n. sp.') {
        taxon_name += species_reso+" "+species_name;
    } else {
        taxon_name += species_name;
    }

    if (isSpecies) {
        taxon_name += "<\/i>";
    }
    
    if (species_reso == 'n. sp.') {
        taxon_name += " n. sp.";
    }
    return taxon_name;
}

function getXOffset() {
    var offset = 0;
    if (navigator.appName == "Microsoft Internet Explorer") {
        offset = document.body.scrollLeft;
    } else {
        offset = window.pageXOffset;
    }
    return offset;
}

function setFixedOffset() {
    offset=(getXOffset() * -1) + 10;
    var head = document.getElementById('occurrencesTableHeader');
    if (head.style.left != offset+'px') {
        head.style.left = offset+'px';
    }
}

setInterval("setFixedOffset()",250);
