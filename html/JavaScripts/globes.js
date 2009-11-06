
	// read in the globe frames
	var globe;
	var globeSrcs = new Array(13)
	var latbox;
	var lngbox;

	function newGlobe(g)	{
		latbox = 3;
		lngbox = 7;
		for ( x = 0; x <= 12; x++ )	{
			globeSrcs[x] = new Array(7)
		}
		globe = g;
		globeSrcs[0][0] = "/public/animations/" + g + "/" + g + "_0_S90.jpg";
		globeSrcs[0][6] = "/public/animations/" + g + "/" + g + "_0_N90.jpg";
		var nS = 1;
		for ( x = 1 ; x <= 12; x++ )	{
			var lng = ( x - 7 ) * 30;
			var lngdir = "";
			if ( lng < 0 )	{
				lngdir = "W";
			} else if ( lng > 0 )	{
				lngdir = "E";
			}
			lng = Math.abs(lng);
			for ( y = 1; y <= 5; y++ )	{
				nS++;
				var lat = ( y - 3 ) * 30;
				var latdir = "";
				if ( lat < 0 )	{
					latdir = "S";
				} else if ( lat > 0 )	{
					latdir = "N";
				}
				lat = Math.abs(lat);
				globeSrcs[x][y] = "/public/animations/" + g + "/" + g + "_" + lngdir + lng + "_" + latdir + lat + ".jpg";
			}
		}
	}

	if (!document.all)	{
		window.captureEvents(Event.KEYPRESS);
		window.onkeyup = arrowGlobe;
	} else	{
		document.onkeyup = arrowGlobe;
	}

	var arrows = 0;

	function arrowGlobe (e)	{
		var code = (window.event) ? event.keyCode : e.keyCode;
	// codes: left = 63234; right = 63235; up = 63232; down = 63233
		if ( ( code > 36 && code < 41 ) || ( code > 63231 && code < 63236 ) )	{
			arrows++;
			if ( arrows == 2 && checkBrowser('safari') )	{
				arrows = 0;
				return;
			}
		}
		if ( code ==  38 || code == 63232 )	{ //38
			latbox++;
			swapView(globe);
		} else if ( code == 40 || code == 63233 )	{
			latbox--;
			swapView(globe);
		} else if ( code == 37 || code == 63234 )	{
			lngbox--;
			swapView(globe);
		} else if ( code == 39 || code == 63235 )	{
			lngbox++;
			swapView(globe);
		}
	}

	var detect = navigator.userAgent.toLowerCase();

	function checkBrowser(string)	{
		place = detect.indexOf(string) + 1;
		thestring = string;
		return place;
	}

	function clickGlobe (lngplus,latplus)	{
		lngbox = lngbox + lngplus;
		latbox = latbox + latplus;
		swapView(globe);
	}

	function swapView (globe_name) {
	// fix for a Safari 1.3 and 2.0 in which duplicate events are sent
	//  when the arrow keys are pressed
		if ( lngbox == 0 )	{
			lngbox = 12;
		} else if ( lngbox == 13 )	{
			lngbox = 1;
		}
		if ( latbox < 0 )	{
			latbox = 1;
			lngbox = lngbox + 6;
			if ( lngbox > 12 )	{
				lngbox = lngbox - 12;
			}
		} else if ( latbox > 6 )	{
			latbox = 5;
			lngbox = lngbox + 6;
			if ( lngbox > 12 )	{
				lngbox = lngbox - 12;
			}
		}
		b = lngbox;
		if ( latbox == 0 )	{
			b = 0;
		} else if ( latbox == 6 )	{
			b = 0;
		}
		document[globe_name].src = globeSrcs[b][latbox];
	}

