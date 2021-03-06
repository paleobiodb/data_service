
// off the shelf
function pausecomp(ms)	{
	ms += new Date().getTime();
	while (new Date() < ms){}
}

function showAll(calledFromCorner)	{
	var alldivs = new Array();
	alldivs = document.getElementsByTagName('div');
	var html;
	var showingBlock = 0;
	for ( i = 0; i < alldivs.length; i++ )	{
		// start showing block
		if ( alldivs[i].id == calledFromCorner )	{
			showingBlock = 1;
			html = alldivs[i].innerHTML;
			html = html.replace( /show all/,"" );
			alldivs[i].innerHTML = html;
		// next block encountered, stop showing block
		} else if ( /show all/.test( alldivs[i].innerHTML ) )	{
			showingBlock = 0;
		} else if ( showingBlock == 1 )	{
			if ( /^hot/.test( alldivs[i].id ) && ! /show all/.test( alldivs[i].id.innerHTML ) )	{
				alldivs[i].style.display = 'block';
				alldivs[i].style.fontSize = '0.7em';
				html = alldivs[i].innerHTML;
				html = html.replace( /showChildren/,"hideChildren"  );
				html = html.replace( /\+/,"hide" );
				alldivs[i].innerHTML = html;
			} else if ( /^t/.test( alldivs[i].id ) )	{
				alldivs[i].style.display = 'block';
			}
		}
	}
} 

function showChildren(taxon_no,childList)	{
	pausecomp(300);
	document.getElementById('hot'+taxon_no).style.fontSize = '0.7em';
	document.getElementById('hot'+taxon_no).innerHTML = '<span onClick="hideChildren(\'' + taxon_no + '\',\'' + childList + '\');">hide</span>';
	var nums = new Array();
	nums = childList.split(/,/);
	for ( i = 0; i < nums.length; i++ )	{
		document.getElementById('t'+nums[i]).style.display = 'block';
	}
	return true;
}

function hideChildren(taxon_no,childList)	{
	pausecomp(300);
	document.getElementById('hot'+taxon_no).style.fontSize = '1em';
	document.getElementById('hot'+taxon_no).innerHTML = '<span onClick="showChildren(\'' + taxon_no + '\',\'' + childList + '\');">+</span>';
	var nums = new Array();
	nums = childList.split(/,/);
	for ( i = 0; i < nums.length; i++ )	{
		document.getElementById('t'+nums[i]).style.display = 'none';
	}
	return true;
}


