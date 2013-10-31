var reconstructMap = {
  "init": function() {
    var height = 500,
        width = 960;

    var projection = d3.geo.hammer()
      .scale(165)
      .translate([width / 2, height / 2])
      .precision(.3);
     
    var path = d3.geo.path()
      .projection(projection);

    var svg = d3.select("#reconstructMap")
      .append("svg")
      .attr("height", height)
      .attr("width", width)
      .append("g")
      .attr("id", "reconstructGroup");

    svg.append("defs").append("path")
      .datum({type: "Sphere"})
      .attr("id", "sphere")
      .attr("d", path);

    svg.append("use")
      .attr("class", "stroke")
      .attr("xlink:href", "#sphere");

    svg.append("use")
      .attr("class", "fill")
      .attr("xlink:href", "#sphere");

    reconstructing = false,
    currentReconstruction = '';

    //attach window resize listener to the window
    /*d3.select(window).on("resize", reconstructMap.resize);
    reconstructMap.resize();*/
    // Moved to navMap.js

    d3.select("#mapSwitch")
      .on("click", function() {
        navMap.refresh("reset");
        document.getElementById("reconstructBox").checked = false;
        document.getElementById("viewByTimeBox").checked = false;

        d3.select(".info")
          .html("")
          .style("display", "none");

        d3.select(".rotate")
          .style("box-shadow", "")
          .style("color", "#000");

        d3.select("#reconstructMap").style("display","none");
        timeScale.unhighlight();
        d3.select("#mapControlCover").style("display", "none");

       /* if (navMap.checkFilters()) {
          d3.select(".filters").style("display", "block");
        }*/

        if(parseInt(d3.select("#map").style("height")) < 1) {
          d3.select("#svgMap").style("display", "block");
        } else {
          d3.select("#map").style("display", "block");
        }
      
      });
  
  },
  "rotate": function(interval) {
    if (interval.nam == currentReconstruction.nam && filters.taxon.name == currentReconstruction.taxon && filters.personFilter.name == currentReconstruction.person) {
      return;
    }
    navMap.filterByTime(interval.nam);

    reconstructing = true;

    navMap.showLoading();
    if (window.navMap && parseInt(d3.select("#map").style("height")) > 0) {
      //reconstructMap.addBBox(interval.mid);
    } 

    d3.select("#reconstructContent").remove();

    d3.select('#interval').text(interval.nam);
    d3.select("#rotationInterval").html(interval.nam);

    d3.select('#age').text("(" + interval.mid + " Ma)");
    d3.select("#rotationYear").html(interval.mid + " Ma");

    if (interval.mid < 201) {
      d3.select("#rotationReference").html("<p>Seton, M., R.D. Müller, S. Zahirovic, C. Gaina, T.H. Torsvik, G. Shephard, A. Talsma, M. Gurnis, M. Turner, S. Maus, M. Chandler. 2012. Global continental and ocean basin reconstructions since 200 Ma. <i>Earth-Science Reviews </i>113:212-270.</p>");
    } else {
      d3.select("#rotationReference").html("<p>Wright, N. S. Zahirovic, R.D. Müller, M. Seton. 2013. Towards community-driven paleogeographic reconstructions: intergrating open-access paleogeographic and paleobiology data with plate tectonics. <i>Biogeosciences </i>10:1529-1541.</p>");
    }
    var svg = d3.select("#reconstructGroup")
        .append("g")
        .attr("id", "reconstructContent");

    var filename = interval.nam.split(' ').join('_'),
        height = 500,
        width = 960;

    var projection = d3.geo.hammer()
      .scale(165)
      .translate([width / 2, height / 2])
      .precision(.3);

    var path = d3.geo.path()
      .projection(projection);


    d3.json("collections/" + filename + ".json", function(error, response) {

      /*response.records.forEach(function(d) {
        d.LatLng = new L.LatLng(d.lat,d.lng)
      });*/
      

      // Add these to the other map immediately

      d3.json("plates/" + filename + ".json", function(er, topoPlates) {
          var geojsonPlates = topojson.feature(topoPlates, topoPlates.objects[filename]);
          
          /*svg.selectAll(".plates")
            .data(geojsonPlates.features)
            .enter().append("path")
            .attr("class", "plates")
            .attr("d", path);*/

          svg.insert("path")
            .datum(geojsonPlates)
            .attr("class", "plates")
            .attr("d", path);

          timeScale.highlight(interval.nam);

          // Switch to reconstruct map now
          if(parseInt(d3.select("#map").style("height")) > 1) {
            d3.select("#map").style("display", "none");
          }
          //d3.select("#mapControls").style("display", "none");
          //d3.select(".filters").style("display", "none");
          d3.select("#svgMap").style("display", "none");
          d3.select("#mapControlCover").style("display", "block");
          d3.select("#reconstructMap").style("display", "block");
          
          reconstructMap.resize();

          d3.select(".info")
            .html('')
            .style("display", "none");

        d3.json("rotatedIntervals/" + filename + ".json", function(err, result) {
          var keys = Object.keys(result.objects),
              key = keys[0];
              rotatedPoints = topojson.feature(result, result.objects[key]);

          if (filters.exist.taxon || filters.exist.personFilter) {
            var url = '/data1.1/colls/summary.json?lngmin=-180&lngmax=180&latmin=-90&latmax=90&level=2&limit=99999';

            url = navMap.parseURL(url); 

            d3.json(url, function(wrong, right) {
              var pbdbData = right.records;

              matches = [];
              pbdbData.forEach(function(d) {
                rotatedPoints.features.forEach(function(e) {
                  if (d.oid == parseInt(e.properties.NAME)) {
                    matches.push(e);
                  }
                });
              });

              matches.forEach(function(d) {
                for (var i=0;i<response.records.length;i++) {
                  if (parseInt(d.properties.NAME) == response.records[i].oid) {
                    d.properties.nco = response.records[i].nco;
                    d.properties.noc = response.records[i].noc;
                    d.properties.oid = response.records[i].oid;
                  }
                }
              });

              reconstructMap.addToMap(matches, interval);
            });
          } else {
            rotatedPoints.features.forEach(function(d) {
              for (var i=0;i<response.records.length;i++) {
                if (parseInt(d.properties.NAME) == response.records[i].oid) {
                  d.properties.nco = response.records[i].nco;
                  d.properties.noc = response.records[i].noc;
                  d.properties.oid = response.records[i].oid;
                }
              }
            });

            reconstructMap.addToMap(rotatedPoints.features, interval);
          }
        }); // End plate callback
      }); // End rotated points callback
    }); // end nonrotated point callback
  },

  "addToMap": function(data, interval) {
    var svg = d3.select("#reconstructContent");

    var height = 500,
        width = 960;

    var scale = d3.scale.linear()
      .domain([1, 4140])
      .range([4, 30]);

    var projection = d3.geo.hammer()
      .scale(165)
      .translate([width / 2, height / 2])
      .precision(.3);

    var path = d3.geo.path()
      .projection(projection);

    svg.selectAll(".points")
      .data(data)
    .enter().append("circle")
      .attr("class", "collection")
      .style("fill", interval.col)
      .attr("r", function(d) { return scale(d.properties.nco); })
      .attr("cx", function(d) {
        var coords = projection(d.geometry.coordinates);
        return coords[0];
      })
      .attr("cy", function(d) {
        var coords = projection(d.geometry.coordinates);
        return coords[1];
      })
      .on("mouseover", function(d) {
        d3.select(".info")
          .html("Bin ID: " + d.properties.oid + "<br>Number of collections: " + d.properties.nco + "<br>Number of occurrences: " + d.properties.noc)
          .style("display", "block");
      })
      .on("click", function(d) {
        d3.select(".info")
          .html("Bin ID: " + d.properties.oid + "<br>Number of collections: " + d.properties.nco + "<br>Number of occurrences: " + d.properties.noc)
          .style("display", "block");
        navMap.openBinModal(d);
      })
      .on("mouseout", function(d) {
        d3.select(".info")
          .html("")
          .style("display", "none");
      });
    reconstructing = false;
    navMap.hideLoading();
    currentReconstruction = {"nam": interval.nam, "taxon": "", "person": ""};
    if (filters.exist.taxon) {
      currentReconstruction.taxon =  filters.taxon.name;
    }
    if (filters.exist.personFilter) {
      currentReconstruction.person = filters.personFilter.name;
    }
    //d3.select(".filters").style("display", "none");
  },

  "addBBox": function(year) {
    d3.selectAll(".rotatedBBox").remove();

    var bounds = map.getBounds(),
        sw = bounds._southWest,
        se = bounds.getSouthEast(),
        ne = bounds._northEast,
        nw = bounds.getNorthWest(),
        zoom = map.getZoom();

     // Make sure bad requests aren't made
    sw.lng = (sw.lng < -180) ? -180 : sw.lng;
    sw.lat = (sw.lat < -90) ? -90 : sw.lat;
    ne.lng = (ne.lng > 180) ? 180 : ne.lng;
    ne.lat = (ne.lat > 90) ? 90 : ne.lat;

    var box = [{"oid":"a", "lat": sw.lat, "lng": sw.lng},{"oid":"b", "lat": nw.lat, "lng": nw.lng},{"oid":"c", "lat": ne.lat, "lng": ne.lng},{"oid":"d", "lat": se.lat, "lng": se.lng}];

    box = navMap.buildWKT(box);

    var gPlatesReqData = '&time=' + year + '&points="' + encodeURI(box) + '"&output=geojson';

    d3.xhr('http://gplates.gps.caltech.edu:8080/recon_points/')
      .header("Content-type", "application/x-www-form-urlencoded")
      .post(gPlatesReqData, function(err, result) {
        if (err) {
          console.log("Gplates error - ", err);
          return alert("GPlates could not complete the request");
        }
        var gPlatesResponse = JSON.parse(result.response);

        var rotatedPoints = gPlatesResponse;

       /* var keys = Object.keys(gPlatesResponse.objects),
            key = keys[0];
            rotatedPoints = topojson.feature(gPlatesResponse, gPlatesResponse.objects[key]);*/

        function compare(a,b) {
          if (a.properties.NAME < b.properties.NAME)
             return -1;
          if (a.properties.NAME > b.properties.NAME)
            return 1;
          return 0;
        }
        rotatedPoints.features.sort(compare);

        var bbox = {"type":"FeatureCollection", "features": [{"geometry":{"type":"Polygon", "coordinates": [[rotatedPoints.features[0].geometry.coordinates, rotatedPoints.features[1].geometry.coordinates, rotatedPoints.features[2].geometry.coordinates, rotatedPoints.features[3].geometry.coordinates, rotatedPoints.features[3].geometry.coordinates]]}, "type": "Feature", "properties": {}}]};

        var svg = d3.select("#reconstructContent");

        var height = 500,
            width = 960;

        var projection = d3.geo.hammer()
            .scale(165)
            .translate([width / 2, height / 2])
            .precision(.1);
         
        var path = d3.geo.path()
            .projection(projection);

        svg.selectAll(".bbox")
          .data(bbox.features)
          .enter().append("path")
          .attr("class", "rotatedBBox")
          .attr("fill", "none")
          .attr("stroke", "#ccc")
          .attr("stroke-dasharray", "5,5")
          .attr("stroke-width", 2)
          .attr("opacity", 1)
          .attr("d", path);
      });
  },
  "resize": function() {
    var width = parseInt(d3.select("#graphics").style("width"));

    var g = d3.select("#reconstructMap").select("svg");

    d3.select("#reconstructGroup")
      .attr("transform", function() {
        /* Firefox hack via https://github.com/wout/svg.js/commit/ce1eb91fac1edc923b317caa83a3a4ab10e7c020 */
        var box;
        try {
          box = g.node().getBBox()
        } catch(err) {
          box = {
            x: g.node().clientLeft,
            y: g.node().clientTop,
            width: g.node().clientWidth,
            height: g.node().clientHeight
          }
        }
        
        if (width > (box.width + 50)) {
          return "scale(" + window.innerHeight/800 + ")translate(" + ((width - box.width)/2) + ",0)";
        } else {
          var svgHeight = ((window.innerHeight * 0.70) - 70),
              mapHeight = (width/970 ) * 500;
          return "scale(" + width/970 + ")translate(0," + (svgHeight - mapHeight)/2 + ")";
        }
      });

    d3.select("#reconstructMap").select("svg")
      .style("height", function(d) {
        return ((window.innerHeight * 0.70) - 70) + "px";
      })
      .style("width", function(d) {
        return width - 15 + "px";
      });

    d3.select("#reconstructMapRefContainer")
      .style("height", function() {
        return parseInt(d3.select("#reconstructMap").style("height")) - 1 + "px";
      });

  }
}
reconstructMap.init();
