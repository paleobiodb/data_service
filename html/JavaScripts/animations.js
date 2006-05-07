
// WARNING: the script assumes that there are exactly 213 frames per animation

	var mapDir = ["","fullsize","fullsize2","eckert","eckert2","northpole","southpole","history","history2"]
	var srcs = new Array(9)
	var cs = [2,2,2,2,2,2,2,2,2]
	var lastc = [0,0,0,0,0,0,0,0,0]
	var intervals = [0,0,0,0,0,0,0,0,0]
	var intervals2 = [0,0,0,0,0,0,0,0,0]
	var maxframes = 213
	var maxframes2 = 30

	for ( x = 0; x <= 8; x++)	{
		srcs [x] = new Array(maxframes+2)
	}

	for ( y = 1; y <= maxframes; y++)	{
		for ( x = 0; x <= 6; x++)	{
			if ( x == 3 || x == 4 )	{
				srcs[x][y] = new Image(432,282);
			} else	{
				srcs[x][y] = new Image(324,315);
			}
			srcs[x][y] = "/public/animations/" + mapDir[x] + "/anim" + y + ".jpg";
		}
	}

	for ( y = 1; y <= maxframes2; y++)	{
		for ( x = 7; x <= 8; x++)	{
			if ( x == 7 || 8 == 4 )	{
				srcs[x][y] = new Image(432,282);
			} else	{
				srcs[x][y] = new Image(324,315);
			}
			srcs[x][y] = "/public/animations/" + mapDir[x] + "/anim" + y + ".jpg";
		}
	}

	function start_animation(firstframe,anim,direction)	{

		if ( cs[anim] > 2 )	{
			cs[anim] = -1;
			clearInterval(intervals[anim]);
			clearInterval(intervals2[anim]);
			return;
		} else if ( cs[anim] == -1 )	{
			cs[anim] = lastc[anim];
		}
		clearInterval(intervals[anim]);
		clearInterval(intervals2[anim]);
		if ( anim <= 6 )	{
			intervals[anim] = setInterval("incrementSource(" + anim + ",'" + direction + "')", 250);
			intervals2[anim] = setInterval("rotateFrame('" + firstframe + "'," + anim + ")",250);
		} else	{
			intervals[anim] = setInterval("incrementSource(" + anim + ",'" + direction + "')", 500);
			intervals2[anim] = setInterval("rotateFrame('" + firstframe + "'," + anim + ")",500);
		}

	}

	function incrementSource(anim,direction)	{

		var animmaxframes = maxframes;
		if ( anim == 7 || anim == 8 )	{
			animmaxframes = maxframes2;
		}

		if ( cs[anim] == animmaxframes && direction != "reverse" )	{
			cs[anim] = 1;
		} else if ( cs[anim] == 1 && direction == "reverse" )	{
			cs[anim] = animmaxframes;
		} else if ( cs[anim] == -1 )	{
			return;
		}
		lastc[anim] = cs[anim];
		if ( direction != "reverse" )	{
			cs[anim]++;
		} else	{
			cs[anim]--;
		}

	}

	function rotateFrame (img_name,anim) {
		document[img_name].src = srcs[anim][cs[anim]];
	}

