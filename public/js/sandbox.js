


// javascript placeholder

function sandbox_request ( display_request ) {
    
    var form = document.getElementById("sandbox_form");
    var records = [];
    var i;
    var content;

    if ( ! (sandbox_sections > 0) ) sandbox_sections = 1;
    
    for ( i=0; i<sandbox_sections; i++ )
    {
	var data = { };
	
	for (var fn of sandbox_fields)
	{
	    var value = form["f" + i + "_"+fn].value;
	    
	    if ( value ) {
		
		if ( value == 'NULL' )
		{
		    data[fn] = null;
		}
		
		else if ( value == 'EMPTY' )
		{
		    data[fn] = '';
		}
		
		else if ( sandbox_json[fn] && /^\[|^\{/.test(value) )
		{
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

	if ( Object.keys(data).length > 0 ) records.push(data);
    }

    if ( records.length > 1 )
    {
	content = JSON.stringify(records);
    }

    else if ( records.length == 1 )
    {
	content = JSON.stringify(records[0]);
    }

    else
    {
	window.alert("Nothing to submit");
	return;
    }
    
    if ( display_request )
    {
	var content_text = content.replace(
			 /[&<>'"]/g,
			 tag =>
			 ({
			     '&': '&amp;',
			     '<': '&lt;',
			     '>': '&gt;',
			     "'": '&#39;',
			     '"': '&quot;'
			 }[tag] || tag)
		     );

	rw = window.open("about:blank", "_blank");
	rw.document.write(content_text);
	rw.document.close();
	rw.document.title = "API Request Body From Sandbox";
	
	last_window = rw;
	return;
    }
    
    var base_url = form["use_test"] && form["use_test"].checked ? "/dtest1.2/" : "/data1.2/";
    
    var post_url = base_url + sandbox_operation + ".json";

    var params = form["ds_params"].value.trim();
    
    if ( params ) post_url = post_url + '?' + params;
    
    $.ajax({ url: post_url,
	     type: "POST",
	     data: content,
	     contentType: "application/json",
	     error: function (jqXHR) {
		 var status = jqXHR.status + ' ' + jqXHR.statusText;
		 var display = confirm("Error: " + status + "\nDisplay result?");
		 
		 if ( display )
		 {
		     var error_text = jqXHR.responseText.replace(
			 /[&<>'"]/g,
			 tag =>
			 ({
			     '&': '&amp;',
			     '<': '&lt;',
			     '>': '&gt;',
			     "'": '&#39;',
			     '"': '&quot;'
			 }[tag] || tag)
		     );
		     
		     rw = window.open("about:blank", "_blank");
		     rw.document.write(error_text);
		     rw.document.close();
		     rw.document.title = "API Response From Sandbox";
		     
		     last_window = rw;
		 }		 
	     },
	     success: function(responseData, statusText, jqXHR) {
		 
		 var result_text = jqXHR.responseText.replace(
			 /[&<>'"]/g,
			 tag =>
			 ({
			     '&': '&amp;',
			     '<': '&lt;',
			     '>': '&gt;',
			     "'": '&#39;',
			     '"': '&quot;'
			 }[tag] || tag)
		     );

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


function sandbox_addtest ( ) {

    var testcontrol_elt = document.getElementById("testcontrol");

    if ( testcontrol_elt )
    {
	testcontrol_elt.innerHTML =
	    "<input type=\"checkbox\" id=\"use_test\" name=\"use_test\" value=\"yes\">\n" +
	    "<label for=\"use_test\"> Use test server<\label>";
    }
}


