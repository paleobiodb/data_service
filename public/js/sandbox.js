


// javascript placeholder

function sandbox_request () {
    
    var data = { };
    
    var form = document.getElementById("sandbox_form");
    
    for (var fn of sandbox_fields)
    {
	var value = form["f_"+fn].value;

	if ( value ) {

	    if ( value == 'NULL' )
	    {
		data[fn] = null;
	    }

	    else if ( value == 'EMPTY' )
	    {
		data[fn] = '';
	    }

	    else if ( /^J\[|^J\{/.test(value) )
	    {
		value = value.substring(1);

		try {
		    data[fn] = JSON.parse(value);
		}

		catch (err) {
		    window.alert("Error in field '" + fn + "': " + err.message);
		    return false;
		}
	    }

	    else
	    {
		data[fn] = value;
	    }
	}
    }
    
    var content = JSON.stringify(data);
    
    var post_url = "/data1.2/" + sandbox_operation + ".json";
    
    if ( sandbox_extra ) post_url = post_url + '?' + sandbox_extra;
    
    $.ajax({ url: post_url,
	     type: "POST",
	     data: content,
	     contentType: "application/json",
	     error: function (jqXHR) {
		 var status = jqXHR.status + ' ' + jqXHR.statusText;
		 var display = confirm("Error: " + status + "\nDisplay result?");
		 
		 if ( display )
		 {
		     var error_text = jqXHR.responseText;
		     
		     rw = window.open("about:blank", "_blank");
		     rw.document.write(error_text);
		     rw.document.close();
		     rw.document.title = "API Response From Sandbox";
		     
		     last_window = rw;
		 }		 
	     },
	     success: function(responseData, statusText, jqXHR) {
		 
		 var result_text = jqXHR.responseText;

		 rw = window.open("about:blank", "_blank");
		 rw.document.write(result_text);
		 rw.document.close();
		 rw.document.title = "API Response From Sandbox";

		 last_window = rw;
	     }
	   });

    // Make sure that the form does not submit.
    
    return false;
}


function sandbox_clear ( ) {

    var form = document.getElementById("sandbox_form");
    
    for (var fn of sandbox_fields)
    {
	form["f_"+fn].value = '';
    }
}
