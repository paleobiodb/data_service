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

    var taxa_by_row = new Array();
    var allRows = document.getElementsByName('row_num');
    // This block interates over all the rows and checks to make sure we're not duplicating anything
    for(var i=0;i<allRows.length;i++) {
        var row_num = allRows[i].value;
        var elem = eval('document.forms[0].taxon_key_'+row_num);
        var taxon_name = elem.value;
        taxa_by_row[row_num] = taxon_name;
    }

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

function insertOccurrenceRow() {
    var table = document.getElementById('occurrencesTable');

    var allRows = document.getElementsByName('row_num');
    var row_count= allRows.length; 
    
    var row = table.insertRow(-1);
    var collections = document.getElementsByName('collection_nos');
    var f = document.forms[0];
    var genus_reso = f.genus_reso.value;
    var genus_name = f.genus_name.value;
    var subgenus_reso = f.subgenus_reso.value;
    var subgenus_name = f.subgenus_name.value;
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
        var collection_no = collections[i].value;
        var onclick = 'onClick="changeCurrentCellInfo('+"'"+taxon_name_escaped+"','"+collection_no+"','"+f.reference_no.value+"','','')"+'"';
        var html = '<input name="abund_value_'+row_count+'_'+collection_no+'" size="3" value="" class="'+inputClass+'" '+onclick+'\/>'
                 + '<input name="abund_unit_'+row_count+'_'+collection_no+'" size="3" value="DEFAULT" type="hidden" \/>'
                 + '<input name="key_type_'+row_count+'_'+collection_no+'" value="occurrence_no" type="hidden" \/>'
                 + '<input name="key_value_'+row_count+'_'+collection_no+'" value="-1" type="hidden" \/>';
        cell.innerHTML = html;
        cell.className="fixedColumn";
    }
    var first_cell = row.insertCell(0);
    first_cell.className="fixedColumn";
    row.className = rowClass;
    var taxon_key_escaped = taxon_key.replace(/"/g,'&quot;'); //"
    first_cell.innerHTML = taxon_name 
                         + ' <input type="hidden" name="row_num" value="'+row_count+'" \/>'
                         + ' <input type="hidden" name="taxon_key_'+row_count+'" value="'+taxon_key_escaped+'" \/>';

    f.genus_name.value="";
    f.subgenus_name.value="";
    f.species_name.value="";
    return true;
}

function changeCurrentCellInfo(taxon_name,collection,reference,read_only,authorizer) {
    var cell_info = document.getElementById('cell_info');
    var html = "";
    html += '&nbsp;Taxon: '+taxon_name+"<br />";
    html += '&nbsp;Collection: <a target="_blank" href="bridge.pl?action=displayCollectionDetails&collection_no='+collection+'">'+collection+"<\/a><br />";
    html += '&nbsp;Reference: <a target="_blank" href="bridge.pl?action=displayReference&reference_no='+reference+'">'+reference+"<\/a><br />";
    if (authorizer != '') {
        if (read_only == "readonly") {
            html += '&nbsp;<span class="red">Authorizer: '+authorizer+'<\/span><br />';
        } else {
            html += '&nbsp;Authorizer: '+authorizer+'<br />';
        }
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
    document.getElementById('occurrencesTableHeader').style.left=offset+'px';
}

setInterval("setFixedOffset()",250);
