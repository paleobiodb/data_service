
// WARNING: the script assumes that there are exactly 213 frames per animation

	var mapDir = ["","homepage","orthographic","orthographic2","eckert","eckert2","northpole","southpole","history","history2","Beijing","London","New_Delhi","New_York","Santa_Barbara","Sao_Paulo","Sydney"]
	var srcs = new Array(17)
	var cs = [2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2]
	var lastc = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
	var intervals = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
	var intervals2 = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
	var maxframes = 213
	var maxframes2 = 30

	function loadFrames(firstframe,x)	{

		srcs [x] = new Array(maxframes+2)

		if ( x != 8 && x != 9 )	{
			for ( y = 1; y <= maxframes; y++)	{
				if ( x == 4 || x == 5 )	{
				//	newImage = new Image(432,282);
				} else	{
				//	newImage= new Image(324,315);
				}
				srcs[x][y] = "/public/animations/" + mapDir[x] + "/anim" + y + ".jpg";
			}
		} else	{
			for ( y = 1; y <= maxframes2; y++)	{
				//var newImage = new Image(432,282);
				srcs[x][y] = "/public/animations/" + mapDir[x] + "/anim" + y + ".jpg";
			}
		}

		startAnimation(firstframe,x);

	}

	function startAnimation(firstframe,anim,direction)	{

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
		if ( anim != 8 && anim != 9 )	{
			intervals[anim] = setInterval("incrementSource(" + anim + ",'" + direction + "')", 500);
			intervals2[anim] = setInterval("rotateFrame('" + firstframe + "'," + anim + ")",500);
		} else	{
			intervals[anim] = setInterval("incrementSource(" + anim + ",'" + direction + "')", 500);
			intervals2[anim] = setInterval("rotateFrame('" + firstframe + "'," + anim + ")",500);
		}

	}

	function incrementSource(anim,direction)	{

		var animmaxframes = maxframes;
		if ( anim == 8 || anim == 9 )	{
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

