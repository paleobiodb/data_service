// Global variables
var map, stamen, stamenLabels, g, exlude;

var filters = {"selectedInterval": "", "personFilter": {"id":"", "name": ""}, "taxon": [], "exist": {"selectedInterval" : false, "personFilter": false, "taxon": false}};

function reconstruct(year) {
  console.log("reconstruct start");

  var requestYear = parseInt((year.eag + year.lag) / 2),
      fill = year.col;

  if(parseInt(d3.select("#map").style("height")) > 0) {
    rotateBbox(requestYear);
  }
 
  var scale = d3.scale.linear()
    .domain([1, 4140])
    .range([4, 30]);

  var zoom = map.getZoom();

  d3.select("#reconstructMap").select("svg").remove();

  if (requestYear < 201) {
    d3.select("#reconstructMapReference").html("<p>Seton, M., R.D. Müller, S. Zahirovic, C. Gaina, T.H. Torsvik, G. Shephard, A. Talsma, M. Gurnis, M. Turner, S. Maus, M. Chandler. 2012. Global continental and ocean basin reconstructions since 200 Ma. <i>Earth-Science Reviews </i>113:212-270.</p>");
  } else {
    d3.select("#reconstructMapReference").html("<p>Wright, N. S. Zahirovic, R.D. Müller, M. Seton. 2013. Towards community-driven paleogeographic reconstructions: intergrating open-access paleogeographic and paleobiology data with plate tectonics. <i>Biogeosciences </i>10:1529-1541.</p>");
  }

  d3.select('#interval').text(year.nam);
  d3.select('#age').text(requestYear + " Ma");

  var filename = year.nam.split(' ').join('_');

  d3.json("collections/" + filename + ".json", function(error, response) {
    console.log("first req made");
    response.records.forEach(function(d) {
      d.LatLng = new L.LatLng(d.lat,d.lng)
    });
    if (zoom < 3) {
      refreshMollweide(response);
    } else {
      d3.selectAll(".bins").remove();
      d3.selectAll(".clusters").remove();
      addBins(response, 2, zoom);
    }

    d3.json("plates/plate" + requestYear + ".json", function(er, topoPlates) {
        console.log("plates loaded");
        var geojsonPlates = topojson.feature(topoPlates, topoPlates.objects["plates" + requestYear]);

        d3.select("#map").style("display", "none");
        d3.select("#svgMap").style("display", "none");
        d3.selectAll(".mapCtrl").style("display", "none");
        d3.select(".filters").style("display", "none");
        d3.select("#reconstructMap").style("display","block");
        timeScale.highlight(year.nam);

      d3.json("rotatedIntervals/" + filename + ".json", function(err, result) {
        var gPlatesResponse = result;

        var keys = Object.keys(gPlatesResponse.objects),
            key = keys[0];
            rotatedPoints = topojson.feature(gPlatesResponse, gPlatesResponse.objects[key]);

        rotatedPoints.features.forEach(function(d) {
          for (var i=0;i<response.records.length;i++) {
            if (parseInt(d.properties.NAME) == response.records[i].oid) {
              d.properties.nco = response.records[i].nco;
              d.properties.noc = response.records[i].noc;
              d.properties.oid = response.records[i].oid;
            }
          }
        });

        var height = window.innerHeight * 0.60,
            width = window.innerWidth;

        var projection = d3.geo.hammer()
            .scale(165)
            .translate([width / 2, height / 2])
            .precision(.1);
         
        var path = d3.geo.path()
            .projection(projection);

        var svg = d3.select("#reconstructMap").append("svg")
          .attr("height", height)
          .attr("width", width);

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

        svg.selectAll(".plates")
          .data(geojsonPlates.features)
          .enter().append("path")
          .attr("class", "plates")
          .attr("d", path);

        svg.selectAll(".points")
          .data(rotatedPoints.features)
          .enter().append("circle")
          .attr("class", "collection")
          .style("fill", fill)
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
          })
          .on("mouseout", function(d) {
            d3.select(".info")
              .html("")
              .style("display", "none");
          });

          // Uncheck the checkbox
          document.getElementById("reconstructBox").checked = false;
          d3.select(".rotate").style("background-color", "");
          document.getElementById("viewByTimeBox").checked = false;
          d3.select(".time").style("background-color", "");
        
      }); // End plate callback
    }); // End rotated points callback
  }); // end nonrotated point callback
}
function rotateBbox(year) {
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

  var requestString = "POINT(" + sw.lat + " " + sw.lng + " a" + "),POINT(" + nw.lat + " " + nw.lng + " b" + "),POINT(" + ne.lat + " " + ne.lng + " c" + "),POINT(" + se.lat + " " + se.lng + " d)";

  var geojson = {"type":"FeatureCollection", "features": [{"geometry":{"type":"Polygon", "coordinates": [[[sw.lat, sw.lng], [nw.lat,nw.lng], [ne.lat,ne.lng], se.lat,se.lng]]}, "type": "Feature", "properties": {}}]};

  requestString = encodeURI(requestString);
  var gPlatesReqData = '&time=' + year + '&points="' + requestString + '"&output=topojson';

  d3.xhr('http://gplates.gps.caltech.edu:8080/recon_points/')
    .header("Content-type", "application/x-www-form-urlencoded")
    .post(gPlatesReqData, function(err, result) {
      if (err) {
        console.log("Gplates error - ", err);
        return alert("GPlates could not complete the request");
      }
      console.log("Got gplates response");
      var gPlatesResponse = JSON.parse(result.response);

      var keys = Object.keys(gPlatesResponse.objects),
          key = keys[0];
          rotatedPoints = topojson.feature(gPlatesResponse, gPlatesResponse.objects[key]);

      function compare(a,b) {
        if (a.properties.NAME < b.properties.NAME)
           return -1;
        if (a.properties.NAME > b.properties.NAME)
          return 1;
        return 0;
      }
      rotatedPoints.features.sort(compare);

      var bbox = {"type":"FeatureCollection", "features": [{"geometry":{"type":"Polygon", "coordinates": [[rotatedPoints.features[0].geometry.coordinates, rotatedPoints.features[1].geometry.coordinates, rotatedPoints.features[2].geometry.coordinates, rotatedPoints.features[3].geometry.coordinates, rotatedPoints.features[3].geometry.coordinates]]}, "type": "Feature", "properties": {}}]};

      var svg = d3.select("#reconstructMap").select("svg");

      var height = window.innerHeight * 0.60,
          width = window.innerWidth;

      var projection = d3.geo.hammer()
          .scale(165)
          .translate([width / 2, height / 2])
          .precision(.1);
       
      var path = d3.geo.path()
          .projection(projection);

      svg.selectAll(".bbox")
        .data(bbox.features)
        .enter().append("path")
        .attr("fill", "none")
        .attr("stroke", "#ccc")
        .attr("stroke-dasharray", "5,5")
        .attr("stroke-width", 2)
        .attr("opacity", 1)
        .attr("d", path);
    });
}

function buildWKT(data) {
  var requestString = "";
  for(var i=0; i<data.length; i++) {
    requestString += "POINT(" + data[i].lat + " " + data[i].lng + " " + data[i].oid + "),"
  }
  requestString = requestString.slice(0, -1);
  requestString = encodeURI(requestString);
  return requestString;
}

function updateFilterList(type) {
  switch(type){
    case "selectedInterval":
      d3.select("#selectedInterval")
        .style("display", "inline-block")
        .html(filters.selectedInterval + '<button type="button" class="close" aria-hidden="true">&times;</button>');
      break;
    case "personFilter":
      d3.select("#personFilter")
        .style("display", "inline-block")
        .html(filters.personFilter.name + '<button type="button" class="close" aria-hidden="true">&times;</button>');
      break;
    case "taxon":
     // url += '&base_id=' + filters.taxon.oid;
      break;
  }
  refreshFilterHandlers();
}

function parseURL(url) {
  var count = 0;
  for (key in filters.exist) {
    if (filters.exist.hasOwnProperty(key)) {
      if (filters.exist[key] == true) {
        switch(key) {
          case "selectedInterval":
            url += '&interval=' + filters.selectedInterval;
            break;
          case "personFilter":
            url += '&person_no=' + filters.personFilter.id;
            break;
          case "taxon":
            url += '&base_id=' + filters.taxon.oid;
            break;
        }
        count += 1;
      }
    }
  }
  if (count > 0) {
    d3.select(".filters").style("display", "block");
  } else {
    d3.select(".filters").style("display", "none");
  }
  return url;
}
function refreshMollweide(data) {
  d3.selectAll(".bins").remove();

  var scale = d3.scale.linear()
    .domain([1, 4240])
    .range([4, 15]);

  var width = window.innerWidth,
    height = window.innerHeight * 0.60;

  var projection = d3.geo.hammer()
      .scale(165)
      .translate([width / 2, height / 2])
      .precision(.1);

  var path = d3.geo.path()
      .projection(projection);

  var projsvg = d3.select("#svgMap").select("svg"),
      zoom = 2;
  // Add the bins to the map
  projsvg.selectAll(".circle")
    .data(data.records)
    .enter().append("circle")
    .attr("class", "bins")
    .style("fill", function(d) { return (interval_hash[d.cxi]) ? interval_hash[d.cxi].col : "#000"; })
    .attr("id", function(d) { return "p" + d.cxi; })
    .attr("r", function(d) { return scale(d.nco)*multiplier(zoom); })
    .attr("cx", function(d) {
      var coords = projection([d.lng, d.lat]);
      return coords[0];
    })
    .attr("cy", function(d) {
      var coords = projection([d.lng, d.lat]);
      return coords[1];
    })
    .on("mouseover", function(d) {
      d3.select(".info")
        .html("Number of collections: " + d.nco + "<br>Number of occurrences: " + d.noc)
        .style("display", "block");
      timeScale.highlight(this);
    })
    .on("click", function(d) {
      d3.select(".info")
        .html("Number of collections: " + d.nco + "<br>Number of occurrences: " + d.noc)
        .style("display", "block");
      timeScale.highlight(this);
    })
    .on("mouseout", function(d) {
      d3.select(".info")
        .html("")
        .style("display", "none");
      timeScale.unhighlight()
    });

}

function datelineQuery(lvl) {
  var bounds = map.getBounds(),
      sw = bounds._southWest,
      ne = bounds._northEast,
      zoom = map.getZoom();

  sw.lng = (sw.lng < -180) ? sw.lng + 360 : sw.lng;
  sw.lat = (sw.lat < -90) ? -90 : sw.lat;
  ne.lng = (ne.lng > 180) ? ne.lng - 360 : ne.lng;
  ne.lat = (ne.lat > 90) ? 90 : ne.lat;

  bounds = map.getBounds();
  var west;
  if (bounds._southWest.lng < -180) {
    west = true;
    ne.lng = 180;
  }
  if (bounds._northEast.lng > 180) {
    west = false;
    sw.lng = -180;
  }

  switch(lvl) {
    case 1: 
      var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=1&limit=999999';
      url = parseURL(url);
      d3.json(url, function(error, response) {
        var data = response;
        data.records.forEach(function(d) {
          if (west) {
            d.LatLng = new L.LatLng(d.lat,d.lng - 360);
          } else {
            d.LatLng = new L.LatLng(d.lat,d.lng + 360);
          }
        });
        addBins(data, 1, zoom);
      });
      break;
    case 2:
      var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=2&limit=99999';
      url = parseURL(url);
      d3.json(url, function(error, response) {
        var data = response;
        data.records.forEach(function(d) {
          if (west) {
            d.LatLng = new L.LatLng(d.lat,d.lng - 360);
          } else {
            d.LatLng = new L.LatLng(d.lat,d.lng + 360);
          }
        });
        addBins(data, 2, zoom);
      });
      break;
    case 3:
      var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/list.json?lngmin=' + sw.lng + '&lngmax='
       + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&limit=99999999';
       url = parseURL(url);
      break;
  }
}
// function that updates what is currently viewed on the map
function refresh(year) {
  // Delete any existing markers
  if (parseInt(d3.select("#map").style("height")) < 1) {
    var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/summary.json?lngmin=-180&lngmax=180&latmin=-90&latmax=90&level=1&limit=999999';
    url = parseURL(url);

    d3.json(url, function(error, data) { 
      refreshMollweide(data);
    });
    return;
  }

  var bounds = map.getBounds(),
      sw = bounds._southWest,
      ne = bounds._northEast,
      zoom = map.getZoom();

  if (prevne.lat > ne.lat && prevne.lng > ne.lng && prevsw.lat < sw.lat && prevsw.lng < sw.lng) {
    if (prevzoom < 4 && zoom > 3) {
      // refresh
    } else if (prevzoom < 7 && zoom > 6) {
      //refresh
    } else {
      var points = d3.selectAll(".bins");
      if (zoom > 6) {
        var clusters = d3.selectAll(".clusters");
        return redraw(points, clusters);
      } else {
        return redraw(points);
      }
      
    }
  }
  prevsw = sw;
  prevne = ne;
  prevzoom = zoom;

  // Make sure bad requests aren't made
  sw.lng = (sw.lng < -180) ? -180 : sw.lng;
  sw.lat = (sw.lat < -90) ? -90 : sw.lat;
  ne.lng = (ne.lng > 180) ? 180 : ne.lng;
  ne.lat = (ne.lat > 90) ? 90 : ne.lat;

  // See if labels should be applied or not
  selectBaseMap(zoom);

  d3.selectAll(".bins").remove();
  d3.selectAll(".clusters").remove();

  bounds = map.getBounds();

  // Depending on the zoom level, call a different service from PaleoDB, feed it a bounding box, and pass it to function getData
  if (zoom < 4) {
    var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=1&limit=999999';

    if (bounds._southWest.lng < -180 || bounds._northEast.lng > 180) {
      datelineQuery(1);
    }
    // Make requests here and pass result to specific functions

    //updateDownloadLink(url, zoom);
    d3.json(parseURL(url), function(error, data) {
      data.records.forEach(function(d) {
        d.LatLng = new L.LatLng(d.lat,d.lng)
      });
      addBins(data, 1, zoom);
    });
  } else if (zoom > 3 && zoom < 7 ) {
    var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=2&limit=99999';

    if (bounds._southWest.lng < -180 || bounds._northEast.lng > 180) {
      datelineQuery(2);
    }

    //updateDownloadLink(url, zoom);
    d3.json(parseURL(url), function(error, data) {
      data.records.forEach(function(d) {
        d.LatLng = new L.LatLng(d.lat,d.lng)
      });
      addBins(data, 2, zoom);
    });
  } else {
    var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/list.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&limit=99999999';

    if (sw.lng < -180 || ne.lng > 180) {
      datelineQuery(3);
    }

    //updateDownloadLink(url, zoom);
    getCollections(parseURL(url), 3, zoom);
  }

} // End function refresh

function selectBaseMap(zoom) {
  if (zoom < 5) {
    if (map.hasLayer(stamenLabels)) {
      map.removeLayer(stamenLabels);
      map.addLayer(stamen);
    }
  } else if (zoom > 4 && zoom < 8) {
    if (map.hasLayer(stamenLabels)) {
      map.removeLayer(stamenLabels);
      map.addLayer(stamen);
    }
  } else {
    if (map.hasLayer(stamenLabels)) {
      map.removeLayer(stamen);
    } else {
      map.addLayer(stamenLabels);
      map.removeLayer(stamen);
    }
  }
}

function addBins(data, level, zoom) {
  // Add the bins to the map
  var points = g.selectAll(".circle")
    .data(data.records)
    .enter().append("circle")
    .attr("class", "bins")
    .style("fill", function(d) { return (interval_hash[d.cxi]) ? interval_hash[d.cxi].col : "#000"; })
    .attr("id", function(d) { return "p" + d.cxi; })
    .on("mouseover", function(d) {
      d3.select(".info")
        .html("Number of collections: " + d.nco + "<br>Number of occurrences: " + d.noc)
        .style("display", "block");
      timeScale.highlight(this);
    })
    .on("click", function(d) {
      d3.select(".info")
        .html("Number of collections: " + d.nco + "<br>Number of occurrences: " + d.noc)
        .style("display", "block");
      timeScale.highlight(this);
    })
    .on("mouseout", function(d) {
      d3.select(".info")
        .html("")
        .style("display", "none");
      timeScale.unhighlight()
    })
    .on("dblclick", function(d) {
      if (level == 1) {
        map.setView(d.LatLng, 6);
      } else if (level == 2) {
        map.setView(d.LatLng, 8);
      }
    });

  // Update the SVG positioning
  redraw(points);
}

function getCollections(source, level, zoom) {
  d3.selectAll(".bins").remove();
  d3.selectAll(".clusters").remove();

  // Make an AJAX request to PaleoDB
  d3.json(source, function(error, data) {
    // Many collections share the same coordinates, making it necessary to create clusters of like coordinates
    var clusters = [];
    // For each collection, check it's coordinates against all others and see if any matches exist
    for (var i=0; i<data.records.length; i++) {
      for (var j=0; j<data.records.length; j++) {
        // If another collection has the same lat/lng and a different OID, create a new cluster
        // SIDENOTE: this could be extended for binning by specifying a tolerance instead of an exact match of coordinates
        if (data.records[i].lat == data.records[j].lat && data.records[i].lng == data.records[j].lng && data.records[i].oid != data.records[j].oid) {
          var newCluster = {"lat":data.records[i].lat, "lng":data.records[i].lng, "members": []};
          var exists = 0;
          // Make sure a cluster with those coordinates doesn't already exist
          for (var z=0; z<clusters.length;z++) {
            if (newCluster.lat == clusters[z].lat && newCluster.lng == clusters[z].lng) {
              exists += 1;
            }
          }
          // If a cluster doesn't already exist with those coordinates, add the cluster to the cluster array
          if (exists < 1) {
            clusters.push(newCluster);
            break;
          // Otherwise, ignore it
          } else {
            break;
          }
        }
      }
    }
    // Loop through all the collections and place them into the proper cluster, if applicable
    // Collections placed into a cluster are kept track of using toRemove. They are not removed from
    // data.records immediately because the length of data.records is being used to count the loop
    // Also keep track of rock formations
    var toRemove = [];
    for (var i=0; i<clusters.length; i++) {
      for (var j=0; j<data.records.length; j++) {
        if (clusters[i].lat == data.records[j].lat && clusters[i].lng == data.records[j].lng) {
          clusters[i].members.push(data.records[j]);
          toRemove.push(data.records[j].oid);
        }
      }
    }
    // Remove all clustered collections from data.records
    for (var i=0; i<toRemove.length; i++) {
      var index = arrayObjectIndexOf(data.records, toRemove[i], "oid");
      data.records.splice(index, 1);
    }
    
    // Create a Leaflet Lat/lng for all collections and clusters
    data.records.forEach(function(d) {
      d.LatLng = new L.LatLng(d.lat,d.lng)
    });

    clusters.forEach(function(d) {
      d.LatLng = new L.LatLng(d.lat,d.lng);
      var clusterBottoms = [],
        clusterTops = [],
        totalOccurrences = [];

      d.members.forEach(function(e) {
        clusterBottoms.push(e.eag);
        clusterTops.push(e.lag);
        totalOccurrences.push(e.noc);
      });
      d.ageTop = d3.min(clusterTops);
      d.ageBottom = d3.max(clusterBottoms);
      // TODO: fix this to something more accurate
      /* Annecdotal evidence suggests all collections that share a lat/lng should be from the 
        same interval, but I doubt that it's always true */
      d.cxi = d.members[0].cxi;
      d.noc = d3.sum(totalOccurrences);
    });

    var clusterPoints = g.selectAll(".clusters")
      .data(clusters)
      .enter().append("circle")
      .attr("class", "clusters")
      .attr("id", function(d) { return "p" + d.members[0].cxi; })
      .style("fill", function(d) { return interval_hash[d.cxi].col; })
      .on("mouseover", function(d) {
        d3.select(".info")
          .html(d.members.length + " collections<br>" + interval_hash[d.cxi].nam + "<br>" + d.noc + " occurrences")
          .style("display", "block");
        timeScale.highlight(this);
      })
      .on("mouseout", function(d) {
        d3.select(".info")
          .html("")
          .style("display", "none");
        timeScale.unhighlight();
      })
      .on("click", function(d) {
        d3.select("#clusterTable")
          .html("");

        d3.select("#window")
          .style("display", "block");

        d3.select(".info")
          .html(d.members.length + " collections<br>" + interval_hash[d.cxi].nam + "<br>" + d.noc + " occurrences")
          .style("display", "block");

        d3.select("#clusterTable")
          .append("tbody")
          .selectAll("tr")
          .data(d.members)
         .enter().append("tr")
          .html(function(e) { return "<td>" + e.nam + "</td>"})
          .on("mouseover", function(e) {
            d3.select(".info")
              .html(e.nam + "<br>" + interval_hash[e.cxi].nam + "<br>" + e.noc + " occurrences")
              .style("display", "block");
            timeScale.highlight(e);
          })
          .on("mouseout", function(e) {
            timeScale.unhighlight();
          })
          .on("click", function(e) {
            d3.select(".info")
              .html(e.nam + "<br>" + interval_hash[e.cxi].nam + "<br>" + e.noc + " occurrences")
              .style("display", "block");
            timeScale.highlight(e);
          });
      });

    var points = g.selectAll(".circle")
      .data(data.records)
      .enter().append("circle")
      .attr("id", function(d) { return "p" + d.cxi })
      .attr("class", "bins")
      .style("fill", function(d) { return (interval_hash[d.cxi]) ? interval_hash[d.cxi].col : "#000"; })
      .on("mouseover", function(d) {
        d3.select(".info")
          .html(d.nam + "<br>" + interval_hash[d.cxi].nam + "<br>" + d.noc + " occurrences")
          .style("display", "block");
        timeScale.highlight(this);
      })
      .on("click", function(d) {
        d3.select(".info")
          .html(d.nam + "<br>" + interval_hash[d.cxi].nam + "<br>" + d.noc + " occurrences")
          .style("display", "block");
        timeScale.highlight(this);
      })
      .on("mouseout", function(d) {
        d3.select(".info")
          .html("")
          .style("display", "none");
        timeScale.unhighlight();
      });

    redraw(points, clusterPoints);
  });
}

// Adjust the positioning of the SVG elements relative to the map frame
function redraw(points, clusterPoints) {
  var zoom = map.getZoom();
  if (zoom < 4) {
    var scale = d3.scale.linear()
      .domain([1, 4140])
      .range([4, 30]);
  } else if (zoom > 3 && zoom < 7 ) {
    var scale = d3.scale.log()
      .domain([1, 400])
      .range([4,30]);
  } else {
    var scale = d3.scale.linear()
      .domain([1, 50])
      .range([12, 30]);
  }
  points.attr("cx",function(d) { return map.latLngToLayerPoint(d.LatLng).x});
  points.attr("cy",function(d) { return map.latLngToLayerPoint(d.LatLng).y});
  if (clusterPoints) {
    clusterPoints.attr("cx",function(d) { return map.latLngToLayerPoint(d.LatLng).x});
    clusterPoints.attr("cy",function(d) { return map.latLngToLayerPoint(d.LatLng).y});
    clusterPoints.attr("r", function(d) { return scale(d.members.length); })
    points.attr("r", 12);
  } else {
    points.attr("r", function(d) { return scale(d.nco)*multiplier(zoom); });
  }
}
// Adjust the size of the markers depending on zoom level
// TODO turn this into a hash
function multiplier(zoom) {
  switch(zoom) {
    case 2:
      return 0.75;
      break; 
    case 3:
      return 1;
      break;
    case 4:
      return 0.5;
      break;
    case 5:
      return 0.8;
      break;
    case 6:
      return 1;
      break;
    case 7:
      return 1.5;
      break;
    default:
      return 1;
      break;
  }
}

// Used for removing items from data.records after they are added to clusters
function arrayObjectIndexOf(myArray, searchTerm, property) {
  for(var i=0, len=myArray.length; i<len; i++) {
    if (myArray[i][property] === searchTerm) return i;
  }
  return -1;
}

function sizeChange() {
  d3.select("#map")
    .style("height", function(d) {
      return window.innerHeight * 0.60 + "px";
    });
  map.invalidateSize();

  d3.select("#svgMap")
    .style("height", function(d) {
      return window.innerHeight * 0.60 + "px";
    });

  d3.select("#reconstructMap")
    .style("height", function(d) {
      return window.innerHeight * 0.60 + "px";
    });

  d3.select("#infoContainer")
    .style("height", function() {
      return window.innerHeight * 0.60 + "px";
    });

  d3.select("#window")
    .style("height", function(d) {
      return parseInt(d3.select("#map").style("height")) - 16 + "px";
    });
  
  d3.selectAll(".taxa")
    .style("height", function(d) {
      //console.log((parseInt(d3.select("#map").style("height")) - 26)/15);
      return (parseInt(d3.select("#map").style("height")) - 26)/15 + "px";
    })
    .style("width", function(d) {
      return (parseInt(d3.select("#map").style("height")) - 26)/15 + "px";
    });

  timeScale.sizeChange();
}

function filterByPerson(name) {
  var person = d3.select("#personInput").property("value");
  d3.json('http://testpaleodb.geology.wisc.edu/data1.1/people/list.json?name=' + person, function(err, result) {
    if (err || result.records.length < 1) {
      return alert("No people with that name found. Please try again.");
    }
    filters.exist.personFilter = true;
    filters.personFilter.id = result.records[0].oid;
    filters.personFilter.name = result.records[0].nam;
    console.log("Filtering by ", filters.personFilter.name);
    updateFilterList("personFilter");
    d3.select(".userToggler").style("display", "none");
    refresh();
    return false;
  });
  return false;
}

function refreshFilterHandlers() {
  d3.selectAll(".close").on("click", function() {
    var parent = d3.select(this).node().parentNode;
    parent = d3.select(parent);
    parent.style("display", "none").html("");
    var type = parent.attr("id");
    filters.exist[type] = false;
    refresh();
  });
}

// Initialize the map and time scale on load
(function() {
  // Time scale
  timeScale.init("timescale");

  prevsw = {"lng": 0, "lat": 0};
  prevne = {"lng": 0, "lat": 0};
  prevzoom = 3;

  // Init the leaflet map
  map = new L.Map('map', {
    center: new L.LatLng(7, 0),
    zoom: 2,
    maxZoom:10,
    minZoom: 2,
    zoomControl: false
  });

  var attrib = 'Map tiles by <a href="http://stamen.com">Stamen Design</a>, <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a> &mdash; Map data © <a href="http://openstreetmap.org">OpenStreetMap</a> contributors';

  stamen = new L.TileLayer('http://{s}.tile.stamen.com/toner-background/{z}/{x}/{y}.png', {attribution: attrib}).addTo(map);

  stamenLabels = new L.TileLayer('http://{s}.tile.stamen.com/toner/{z}/{x}/{y}.png', {attribution: attrib});

  map.on("moveend", function() {
    d3.select("#window").style("display", "none");
    //d3.select(".layerToggler").style("display", "none");
    d3.selectAll("rect").style("stroke", "#fff");
    d3.select(".info").style("display", "none");

    var zoom = map.getZoom();
    if (zoom < 3) {
      mapHeight = d3.select("#map").style("height");
      d3.select("#map").style("height", 0);
      d3.select("#svgMap").style("display", "block");
      d3.selectAll("path").attr("d", path);
    }

    refresh();
    
  });
  d3.select("#map").style("height", 0);

  var width = window.innerWidth,
      height = window.innerHeight * 0.60;
  
  var projection = d3.geo.hammer()
    .scale(165)
    .translate([width / 2, height / 2])
    .precision(.1);

  var mercator = d3.geo.mercator()
    .scale(165)
    .precision(.1)
    .translate([width / 2, height / 2]);

  var path = d3.geo.path()
    .projection(projection);

  function changeMaps(mouse) {
    var coords = mouse,
      projected = mercator.invert(coords);

    d3.select("#svgMap").style("display", "none");
    d3.select("#map").style("height", mapHeight);
    map.invalidateSize();

    map.setView([parseInt(projected[1]), parseInt(projected[0])], 3, {animate:false});
  }

  var zoom = d3.behavior.zoom()
    .on("zoom",function() {
      if (d3.event.sourceEvent.wheelDelta > 0) {
        changeMaps(d3.mouse(this));
      }
    });

  var projsvg = d3.select("#svgMap").append("svg")
    .attr("width", width)
    .attr("height", height)
    .call(zoom)
    .on("click", function() {
      changeMaps(d3.mouse(this));
    });

  projsvg.append("defs").append("path")
    .datum({type: "Sphere"})
    .attr("id", "sphere")
    .attr("d", path);

  projsvg.append("use")
    .attr("class", "fill")
    .attr("xlink:href", "#sphere");

  d3.json("countries_1e5.json", function(error, data) {
    projsvg.append("path")
      .datum(topojson.feature(data, data.objects.countries))
      .attr("class", "countries")
      .attr("d", path);

  });
 /*
  var taxaToggler = L.control({position: 'topright'});
  taxaToggler.onAdd = function(map) {
    var div = L.DomUtil.create('div', 'customControl taxaToggler');
    div.innerHTML += '<div data-toggle="tooltip" title="Tyranasaurus" data-placement="left" class="taxa" id="trex"></div>';
    div.innerHTML += '<br><div data-toggle="tooltip" title="Bacteria" data-placement="left" class="taxa" id="bacteria"></div>';
    div.innerHTML += '<br><div data-toggle="tooltip" title="Lecanorales" data-placement="left" class="taxa" id="lecanorales"></div>';
    div.innerHTML += '<br><div data-toggle="tooltip" title="Opisthokonta" data-placement="left" class="taxa" id="opisthokonta"></div>';
    div.innerHTML += '<br><div data-toggle="tooltip" title="Plant" data-placement="left" class="taxa" id="plant"></div>';
    div.innerHTML += '<br><div data-toggle="tooltip" title="Rotaria" data-placement="left" class="taxa" id="rotaria"></div>';
    div.innerHTML += '<br><div data-toggle="tooltip" title="Starfish" data-placement="left" class="taxa" id="starfish"></div>';
    div.innerHTML += '<br><div data-toggle="tooltip" title="Thysanura" data-placement="left" class="taxa" id="thysanura"></div>';
    div.innerHTML += '<br><div data-toggle="tooltip" title="Tree" data-placement="left" class="taxa" id="tree"></div>';
    div.innerHTML += '<br><div data-toggle="tooltip" title="Trilobita" data-placement="left" class="taxa" id="trilobita"></div>';
    return div;
  }
  taxaToggler.addTo(map);

  $('.taxa').tooltip();*/

  // Attach handlers for zoom-in and zoom-out buttons
  d3.select(".zoom-in").on("click", function() {
    if (parseInt(d3.select("#map").style("height")) < 1) {
      d3.event.stopPropagation();
      d3.select("#svgMap").style("display", "none");
      d3.select("#map").style("height", mapHeight);
      map.invalidateSize();

      map.setView([7,0], 3, {animate:false});
    } else {
      map.zoomIn();
    }
  });
  d3.select(".zoom-out")
    .on("click",function() {
      map.zoomOut();
    });

  d3.select("#mapSwitch")
    .on("click", function() {
      d3.select("#map").style("display", "block");
      d3.select(".filters").style("display", "block");
      //d3.select("#mapControls").style("display", "block");
      d3.selectAll(".mapCtrl").style("display", "block");
      d3.select("#reconstructMap").style("display","none");
      timeScale.unhighlight();

      if(parseInt(d3.select("#map").style("height")) < 1) {
        d3.select("#svgMap").style("display", "block");
      }
    });

  d3.select(".tools")
    .on("click", function() {
      var visible = d3.select(".toolToggler").style("display");
      if (visible == "block") {
        d3.select(".toolToggler").style("display", "none");
      } else {
        d3.select(".toolToggler").style("display", "block");
      }
    });

  d3.select(".rotate")
    .on("click", function() {
      var rotateChecked = document.getElementById("reconstructBox").checked,
          timeChecked = document.getElementById("viewByTimeBox").checked;
      if (rotateChecked == true) {
        console.log("unchecking box");
        document.getElementById("reconstructBox").checked = false;
        d3.select(".rotate").style("background-color", "");
      } else {
        if (timeChecked == false) {
          document.getElementById("viewByTimeBox").checked = true;
          d3.select(".time").style("background-color", "#ccc");
        }
        console.log("checking box");
        document.getElementById("reconstructBox").checked = true;
        d3.select(".rotate").style("background-color", "#ccc");
        //filters.selectedInterval = '';
        //refresh();
      }
    });

  d3.select(".time")
    .on("click", function() {
      var checked = document.getElementById("viewByTimeBox").checked;
      if (checked == true) {
        console.log("unchecking box");
        document.getElementById("viewByTimeBox").checked = false;
        d3.select(".time").style("background-color", "");
      } else {
        console.log("checking box");
        document.getElementById("viewByTimeBox").checked = true;
        d3.select(".time").style("background-color", "#ccc");
        filters.selectedInterval = '';
      }
    });

  d3.select(".userFilter")
    .on("click", function() {
      var visible = d3.select(".userToggler").style("display");
      if (visible == "block") {
        d3.select(".userToggler").style("display", "none");
      } else {
        d3.select(".userToggler").style("display", "block");
      }
    });

  d3.select("#personFilterBox")
    .on("click", function() {
      var checked = document.getElementById("personFilterBox").checked;
      if (checked !== true) {
        filters.personFilter = '';
        refresh();
      }
    });
/*
  d3.select(".taxaToggler")
    .on("click", function() {
      console.log("clicked");
      var visible = d3.select("#taxaBrowser").style("display");
      if (visible == "block") {
        d3.select("#taxaBrowser").style("display", "none");
        d3.select("#graphics").attr("class", "col-lg-12");
        sizeChange();
      } else {
        d3.select("#taxaBrowser").style("display", "block");
        d3.select("#graphics").attr("class", "col-lg-9");
        sizeChange();
      }
    });*/

  $('.rotate').tooltip({
    placement:'right',
    title:'Click, then select interval to rotate to paleo coordinates'
  });
  $('.time').tooltip({
    placement:'right',
    title:'Click, then select interval to filter map'
  });
  $('.userFilter').tooltip({
    placement:'right',
    title:'Filter by user'
  });
  $('.layers').tooltip({
    placement:'right',
    title:'Change symbology'
  });

  //attach window resize listener to the window
  d3.select(window).on("resize", sizeChange);
  
  // Get map ready for an SVG layer
  map._initPathRoot();

  // Add the SVG to hold markers to the map
  var svg = d3.select("#map").select("svg");
  g = svg.append("g");

  function hideMap() {
    d3.select("#reconstructMap").style("display", "none");
  }
  setTimeout(hideMap, 500);

 // refresh();
  setTimeout(sizeChange, 100);
  setTimeout(sizeChange, 100);
})();