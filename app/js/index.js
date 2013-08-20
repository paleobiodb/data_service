// D3 hack to make sure rectangles in the time scale highlight properly
d3.selection.prototype.moveToFront = function() {
  return this.each(function(){
    this.parentNode.appendChild(this);
  });
};

// Global variables
var mapType, map, stamen, stamenLabels, g, interval_hash, exlude;

function reconstruct(year) {
  // TODO: 
  // 3. Break large requests up into multiple

  //updateAuthors();

  console.log("reconstruct start");

  var bounds = map.getBounds(),
      sw = bounds._southWest,
      ne = bounds._northEast,
      zoom = map.getZoom();

   // Make sure bad requests aren't made
  sw.lng = (sw.lng < -180) ? -180 : sw.lng;
  sw.lat = (sw.lat < -90) ? -90 : sw.lat;
  ne.lng = (ne.lng > 180) ? 180 : ne.lng;
  ne.lat = (ne.lat > 90) ? 90 : ne.lat;

  var requestYear = parseInt((year.eag + year.lag) / 2),
      fill = year.col;

  if (Object.keys(reconstructMap._layers).length > 1) {
    for(i in reconstructMap._layers) {
      if(reconstructMap._layers[i]._path != undefined) {
        reconstructMap.removeLayer(reconstructMap._layers[i]);
      }
    }
  }

  d3.select('#age')
    .text(requestYear + " Ma");
 
  if (zoom < 5) {
    var type = 'clusters';
    var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=1&limit=9999999&interval=' + year.nam;
    if (typeof personFilter != 'undefined') {
      url += '&person_no=' + personFilter;
    }
    if (typeof taxon != 'undefined') {
      url += '&base_id=' + taxon.oid;
    }
  } else if (zoom > 4 && zoom < 8 ) {
    var type = 'clusters';
    var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=2&limit=999999&interval=' + year.nam;
    if (typeof personFilter != 'undefined') {
      url += '&person_no=' + personFilter;
    }
    if (typeof taxon != 'undefined') {
      url += '&base_id=' + taxon.oid;
    }
  } else {
    var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/list.json?lngmin=' + sw.lng + '&lngmax='
       + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&max_ma=' + year.eag + '&min_ma=' + year.lag + '&limit=999999';
    if (typeof personFilter != 'undefined') {
      url += '&person_no=' + personFilter;
    }
    if (typeof taxon != 'undefined') {
      url += '&base_id=' + taxon.oid;
    }
  }

  var keepers = [];

  d3.json(url, function(error, response) {
    console.log("first req made");
//////////
/*    var numberPoints = response.records.length;
    var numberOfRequests = Math.ceil(numberPoints / 21);

    var start = 0,
        end = 21;

    var rotatedPoints ={"type": "FeatureCollection", "features": []};
    for(var j=0; j < numberOfRequests; j++) {
      var subset = [];
      for(var k=start; k < end; k++) {
        subset.push(response.records[k]);
      }
      var gPlatesReq = 'http://gplates.gps.caltech.edu:8080/recon_points/?time=' + requestYear + '&points="' + buildWKT(subset) + '"&output=topojson';
      d3.json(gPlatesReq, function(err, result) {
        var keys = Object.keys(result.objects),
            key = keys[0],
            rotatedSubset = topojson.feature(result, result.objects[key]);

        rotatedPoints.features.push(rotatedSubset.features);
      });
      start += 21;
      end += 21;
    }*/
////////////
    //var shortResponse = [];
    //for (var i = 0; i < 20; i++) {
    //  shortResponse.push(response.records[i])
    //};
    //var gPlatesReq = 'http://gplates.gps.caltech.edu:8080/recon_points/?time=' + requestYear + '&points="' + buildWKT(shortResponse) + '"&output=geojson';
    //var gPlatesReq = 'http://gplates.gps.caltech.edu:8080/recon_points/?time=' + requestYear + '&points="' + buildWKT(response.records) + '"&output=geojson';

    var gPlatesReqData = '&time=' + requestYear + '&points="' + buildWKT(response.records) + '"&output=geojson';

    d3.xhr('http://gplates.gps.caltech.edu:8080/recon_points/')
      .header("Content-type", "application/x-www-form-urlencoded")
      .post(gPlatesReqData, function(err, result) {
        if (err) {
          console.log("Gplates error - ", err);
          return alert("GPlates could not complete the request");
        }
        var gPlatesResponse = JSON.parse(result.response);
        /*var keys = Object.keys(gPlatesResponse.objects),
            key = keys[0];
            rotatedPoints = topojson.feature(gPlatesResponse, gPlatesResponse.objects[key]);*/
            rotatedPoints = gPlatesResponse;
/*
      });

    d3.json(gPlatesReq, function(err, resp) {
      if (err) {
        console.log("Gplates error - ", err);
        return alert("GPlates could not complete the request");
      }
     // var keys = Object.keys(resp.objects),
     //     key = keys[0];
         // rotatedPoints = topojson.feature(resp, resp.objects[key]);
      rotatedPoints = resp;*/

      d3.json("plates/plate" + requestYear + ".json", function(er, topoPlates) {
        var geojsonPlates = topojson.feature(topoPlates, topoPlates.objects["plates" + requestYear]);
        d3.select("#map").style("display", "none");
        d3.select("#reconstructMap").style("display","block");
        plateLayer = new L.geoJson(geojsonPlates, {
          style: {
            "color": "#000",
            "fillColor": "#FEFBFB",
            "weight": 1,
            "fillOpacity": 1
          }
        }).addTo(reconstructMap);

        pointLayer = new L.geoJson(rotatedPoints, {
          pointToLayer: function(feature, latlng) {
            return L.circleMarker(latlng, {
              radius: 10
            });
          },
          style: {
            "color" : fill,
            "weight": 0,
            "fillColor" : fill,
            "fillOpacity" : 0.7
          }
        }).addTo(reconstructMap);

        lastRequestYear = requestYear;

        // Uncheck the checkbox
        document.getElementById("reconstructBox").checked = false;
        
      });
      
    });

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

function updateAuthors() {
  var bounds = map.getBounds(),
      sw = bounds._southWest,
      ne = bounds._northEast,
      zoom = map.getZoom();

   // Make sure bad requests aren't made
  sw.lng = (sw.lng < -180) ? -180 : sw.lng;
  sw.lat = (sw.lat < -90) ? -90 : sw.lat;
  ne.lng = (ne.lng > 180) ? 180 : ne.lng;
  ne.lat = (ne.lat > 90) ? 90 : ne.lat;

  var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/toprank.json?show=author&lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&limit=6';

  if (typeof selectedInterval != 'undefined') {
    url += '&interval=' + selectedInterval;
  }
  if (typeof personFilter != 'undefined') {
    url += '&person_no=' + personFilter;
  }
  if (typeof taxon != 'undefined') {
    url += '&base_id=' + taxon.oid;
  }
  if (typeof exclude != 'undefined') {
    url += '&exclude_id=' + exclude.oid;
  }

  if (typeof selectedInterval != 'undefined') {
      url += '&interval=' + selectedInterval;
    }
  d3.json(url, function(err, result) {
    d3.select('.credits').html('');
    for(var i=1;i<6;i++) {
      if (i == 5) {
        d3.select('.credits')
          .append('p').attr('class', 'rank').text(result.records[i].aut);
      } else {
        d3.select('.credits')
          .append('p').attr('class', 'rank').text(result.records[i].aut + " | ");
      }
    }
  });
}

// function that updates what is currently viewed on the map
function refresh(year) {
  updateAuthors();
  // Delete any existing markers
  d3.selectAll(".bins").remove();
  d3.selectAll(".clusters").remove();
  // Clear the heatmap, if it exists
  if (Object.keys(map._layers).length > 1) {
    for(i in map._layers) {
      if(map._layers[i]._path != undefined) {
        map.removeLayer(map._layers[i]);
      }
    }
  }

  var bounds = map.getBounds(),
      sw = bounds._southWest,
      ne = bounds._northEast,
      zoom = map.getZoom();

  // Make sure bad requests aren't made
  sw.lng = (sw.lng < -180) ? -180 : sw.lng;
  sw.lat = (sw.lat < -90) ? -90 : sw.lat;
  ne.lng = (ne.lng > 180) ? 180 : ne.lng;
  ne.lat = (ne.lat > 90) ? 90 : ne.lat;

  // See if labels should be applied or not
  selectBaseMap(zoom);
  if (mapType == "points") {
      // Depending on the zoom level, call a different service from PaleoDB, feed it a bounding box, and pass it to function getData
    if (zoom < 4) {
      var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=1&limit=999999';
      if (typeof selectedInterval != 'undefined') {
        url += '&interval=' + selectedInterval;
      }
      if (typeof personFilter != 'undefined') {
        url += '&person_no=' + personFilter;
      }
      if (typeof taxon != 'undefined') {
        url += '&base_id=' + taxon.oid;
      }
      if (typeof exclude != 'undefined') {
        url += '&exclude_id=' + exclude.oid;
      }
      getBins(url, 1, zoom);
    } else if (zoom > 3 && zoom < 7 ) {
      var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=2&limit=99999';
      if (typeof selectedInterval != 'undefined') {
        url += '&interval=' + selectedInterval;
      }
      if (typeof personFilter != 'undefined') {
        url += '&person_no=' + personFilter;
      }
      if (typeof taxon != 'undefined') {
        url += '&base_id=' + taxon.oid;
      }
      if (typeof exclude != 'undefined') {
        url += '&exclude_id=' + exclude.oid;
      }
      getBins(url, 2, zoom);
    } else {
      var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/list.json?lngmin=' + sw.lng + '&lngmax='
         + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&limit=99999999';
      if (typeof selectedInterval != 'undefined') {
        url += '&interval=' + selectedInterval;
      }
      if (typeof personFilter != 'undefined') {
        url += '&person_no=' + personFilter;
      }
      if (typeof taxon != 'undefined') {
        url += '&base_id=' + taxon.oid;
      }
      if (typeof exclude != 'undefined') {
        url += '&exclude_id=' + exclude.oid;
      }
      getCollections(url, 3, zoom);
    }
  } else {
    if (zoom < 5) {
      buildHeatMap(1, 5);
    } else {
      buildHeatMap(2, 0.5);
    }
  }
  
} // End function update

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

function buildHeatMap(level, degr) {
  var bounds = map.getBounds(),
      sw = bounds._southWest,
      ne = bounds._northEast;

  // Make sure bad requests aren't made
  sw.lng = (sw.lng < -180) ? -180 : sw.lng;
  sw.lat = (sw.lat < -90) ? -90 : sw.lat;
  ne.lng = (ne.lng > 180) ? 180 : ne.lng;
  ne.lat = (ne.lat > 90) ? 90 : ne.lat;

  var source = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=' + level + '&limit=9999999';

  d3.json(source, function(error, data) {
    var bins = data.records;

    var grid = {"type":"FeatureCollection","features":[]};

    for (var i = Math.floor(sw.lng); i < ne.lng ; i += degr) {
      for (var j = Math.floor(sw.lat); j < ne.lat; j += degr) {
        var square = {"type":"Feature","id": getID(i, j, level, degr),"properties":{"id": getID(i, j, level, degr)},"geometry":{"type":"Polygon","coordinates":[[[i,j],[degr+i, j],[degr+i, degr+j],[i,degr+j]]]}};

        for (var z = 0; z < bins.length; z++) {
          if (bins[z].oid == square.properties.id) {
            square.properties.noc = bins[z].noc;
            square.properties.nco = bins[z].nco;
          }
        }
        grid.features.push(square);
      }
    }

    var geojson = L.geoJson(grid, {
      style: function(feature) {
        if (feature.properties.nco) {
          return {color:"#777", weight: 0, fillColor: getFill(feature.properties.noc, level), fillOpacity: 0.5};
        } else {
          return {color:"#fff", weight: 0, fillColor: "#fff", fillOpacity: 0.0};
        }
      }
    }).addTo(map);
  });
}
// get the fill shade for the heat map
function getFill(d, level) {
  var minColor = '#f7fcf5',
      maxColor = '#00441b';

  switch(level) {
    case 1:
      var scale = d3.scale.linear()
        .domain([0, 1000])
        .range([minColor, maxColor]);
      var color = (scale(d) != '#000000') ? scale(d) : maxColor;
      return (scale(d) != '#000000') ? scale(d) : maxColor;

    case 2:
      var scale = d3.scale.pow()
        .domain([0, 600])
        .range([minColor, maxColor]);
      return (scale(d) != '#000000') ? scale(d) : maxColor;
  }
}
// Calculate bin ID
function getID(x, y, level, degr) {
  switch(level) {
    case 1:
      var id = 1000000 + Math.floor((x+180)/degr) * 1000 + Math.floor((y+90)/degr);
      return id;
    case 2:
      var id = 200000000 + Math.floor((x+180)/degr) * 10000 + Math.floor((y+90)/degr);
      return id;
  } 
}

function getBins(source, level, zoom) {
  d3.json(source, function(error, response) {
    var data = response;
    // Create a Leaflet LatLng object for each bin
    data.records.forEach(function(d) {
      d.LatLng = new L.LatLng(d.lat,d.lng)
    });
    // Create a different scale depending on the level of binning
    if (level == 1) {
      var scale = d3.scale.linear()
        .domain([1, 4140])
        .range([4, 30]);
    } else if (level == 2) {
      var scale = d3.scale.log()
        .domain([1, 400])
        .range([4,30]);
    }
    // Add the bins to the map
    var points = g.selectAll(".circle")
      .data(data.records)
      .enter().append("circle")
      .attr("class", "bins")
      .style("fill", function(d) { return (interval_hash[d.cxi]) ? interval_hash[d.cxi].col : "#000"; })
      .attr("id", function(d) { return "p" + d.cxi; })
      .attr("r", function(d) { return scale(d.nco)*multiplier(zoom); })
      .on("mouseover", function(d) {
        d3.select(".info")
          .html("Number of collections: " + d.nco + "<br>Number of occurrences: " + d.noc)
          .style("display", "block");
        highlightRect(this);
      })
      .on("click", function(d) {
        d3.select(".info")
          .html("Number of collections: " + d.nco + "<br>Number of occurrences: " + d.noc)
          .style("display", "block");
        highlightRect(this);
      })
      .on("mouseout", function(d) {
        d3.select(".info")
          .html("")
          .style("display", "none");
        unhighlightRect(this);
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
  });
}

function getCollections(source, level, zoom) {
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

    var scale = d3.scale.linear()
      .domain([1, 50])
      .range([12, 30]);

    var clusterPoints = g.selectAll(".clusters")
      .data(clusters)
      .enter().append("circle")
      .attr("class", "clusters")
      .attr("id", function(d) { return "p" + d.members[0].cxi; })
      .style("fill", function(d) { return interval_hash[d.cxi].col; })
      .attr("r", function(d) { return scale(d.members.length); })
      .on("mouseover", function(d) {
        d3.select(".info")
          .html(d.members.length + " collections<br>" + interval_hash[d.cxi].nam + "<br>" + d.noc + " occurrences")
          .style("display", "block");
        highlightRect(this);
      })
      .on("mouseout", function(d) {
        d3.select(".info")
          .html("")
          .style("display", "none");
        unhighlightRect(this);
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
            highlightRect(e, true);
          })
          .on("mouseout", function(e) {
            unhighlightRect(e, true);
          })
          .on("click", function(e) {
            d3.select(".info")
              .html(e.nam + "<br>" + e.int)
              .style("display", "block")
            highlightRect(this);
          });
      });

    var points = g.selectAll(".circle")
      .data(data.records)
      .enter().append("circle")
      .attr("id", function(d) { return "p" + d.cxi })
      .attr("class", "bins")
      .attr("r", 12)
      .style("fill", function(d) { return interval_hash[d.cxi].col; })
      .on("mouseover", function(d) {
        d3.select(".info")
          .html(d.nam + "<br>" + interval_hash[d.cxi].nam + "<br>" + d.noc + " occurrences")
          .style("display", "block");
        highlightRect(this);
      })
      .on("click", function(d) {
        d3.select(".info")
          .html(d.nam + "<br>" + interval_hash[d.cxi].nam + "<br>" + d.noc + " occurrences")
          .style("display", "block");
        highlightRect(this);
      })
      .on("mouseout", function(d) {
        d3.select(".info")
          .html("")
          .style("display", "none");
        unhighlightRect(this);
      });

    redraw(points, clusterPoints, "collections");
  });
}
/*
d3.xhr('http://gplates.gps.caltech.edu:8080/recon_points/')
  .header("Content-type", "application/x-www-form-urlencoded")
  .post('?time=69&points="POINT(-44.0287%20-89.476%201000009)"&output=topojson', function(err, result) {console.log(result.response)});

d3.json('http://gplates.gps.caltech.edu:8080/recon_points/', function(err, result) {
  console.log(result);
})
.header("Content-Type","application/x-www-form-urlencoded")
.send('POST', '?time=69&points="POINT(-44.0287%20-89.476%201000009)"&output=topojson');

curl -i --data 'time=80&points="POINT(-44 -89 testpt)"&output=geojson' 'http://gplates.gps.caltech.edu:8080/recon_points/'*/

// TODO change these functions to accept an ID instead of a fill
function highlightRect(d, table) {
  // TODO highlight using lti instead of cxi - highlight all intervals contained in collection
  // Make sure everything is reset (especially important for touch interfaces that don't have a mouseout)
  d3.selectAll("rect").style("stroke", "#fff");
  if (table) {
    d3.selectAll("rect#t" + d.cxi).style("stroke", "#000").moveToFront();
    d3.selectAll("#l" + d.cxi).moveToFront();
    d3.selectAll(".abbr").moveToFront();
  } else {
    var id = d3.select(d).attr("id");
    id = id.replace("p", "");
    d3.selectAll("rect#t" + id).style("stroke", "#000").moveToFront();
    d3.selectAll("#l" + id).moveToFront();
    d3.selectAll(".abbr").moveToFront();
  }
}
function unhighlightRect(d, table) {
  if (table) {
    d3.selectAll("rect#t" + d.cxi).style("stroke", "#fff");
  } else {
    var id = d3.select(d).attr("id");
    id = id.replace("p", "");
    d3.selectAll("rect#t" + id).style("stroke", "#fff");
  }
}
// Adjust the positioning of the SVG elements relative to the map frame
function redraw(points, clusterPoints, type) {
  points.attr("cx",function(d) { return map.latLngToLayerPoint(d.LatLng).x});
  points.attr("cy",function(d) { return map.latLngToLayerPoint(d.LatLng).y});
  if (type == "collections") {
    clusterPoints.attr("cx",function(d) { return map.latLngToLayerPoint(d.LatLng).x});
    clusterPoints.attr("cy",function(d) { return map.latLngToLayerPoint(d.LatLng).y});
  }
}
// Adjust the size of the markers depending on zoom level
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

/////////////////////

function buildTimeChart() {
  var w = 960,
      h = 180,
      x = d3.scale.linear().range([0, w]),
      y = d3.scale.linear().range([0, h]);

  // Create the SVG for the chart
  var time = d3.select("#chart").append("svg:svg")
      .attr("width", w)
      .attr("height", h)
      .attr("id", "geologicTime")
      .append("g")
      .attr("id", "chartGroup");

  // Load the time scale data
  d3.json("http://testpaleodb.geology.wisc.edu/data1.1/intervals/list.json?order=older&max_ma=4000", function(error, result) {
    var data = { oid: 0, col: "#000000", nam: "Geologic Time", children: [] };
    interval_hash = { 0: data };
    for(var i=0; i < result.records.length; i++) {
      var r = result.records[i];
      r.children = [];
      r.pid = r.pid || 0;
      r.abr = r.abr || r.nam.charAt(0); 
      r.total = r.eag - r.lag;
      interval_hash[r.oid] = r;
      interval_hash[r.pid].children.push(r);
    }
      // Create a new d3 partition layout
    var partition = d3.layout.partition()
        .sort(function(d) { d3.ascending(d)})
        .value(function(d) { return d.total; });
    // Create the rectangles
    var rect = time.selectAll("rect")
        .data(partition.nodes(data))
      .enter().append("svg:rect")
        .attr("x", function(d) { return x(d.x); })
        .attr("y", function(d) { return y(d.y); })
        .attr("width", function(d) { return x(d.dx); })
        .attr("height", function(d) { return y(d.dy); })
        .attr("fill", function(d) { return d.col || "#000000" })
        .attr("id", function(d) { return "t" + d.oid; })
        .on("click", rectTrans);
    
      // Add the full labels
    var labelsFull = time.selectAll("fullName")
        .data(partition.nodes(data))
      .enter().append("svg:foreignObject")
        .attr("x", function(d) { return labelX(d) })
        .attr("y", function(d) { return y(d.y) + 3;})
        .attr("width", function(d) { return 150; })
        .attr("height", function(d) { return y(d.dy); })
        .text(function(d) { return d.nam; })
        .attr("class", function(d) { return "fullName level" + d.lvl; })
        .attr("id", function(d) { return "l" + d.oid; })
        .on("click", rectTrans);

      // Add the abbreviations
    var labelsAbbr = time.selectAll("abbreviations")
        .data(partition.nodes(data))
      .enter().append("svg:foreignObject")
        .attr("x", function(d) { return labelAbbrX(d); })
        .attr("y", function(d) { return y(d.y) + 3;})
        .attr("width", function(d) { return 30; })
        .attr("height", function(d) { return y(d.dy); })
        .text(function(d) { return d.abr || d.nam.charAt(0); })
        .attr("class", function(d) { return "abbr level" + d.lvl; })
        .on("click", rectTrans);

    // Position the labels for the first time
    labelLevels(interval_hash[0]);

    // Otherwise it's black on black...
    d3.selectAll('.fullName').filter(function(d, i) {
      return d.nam == "Geologic Time";
    }).style("color", "#fff"); 
    // Open to Phanerozoic 
    rectTrans(interval_hash[751])
  }); // End json callback
} // end buildTimeChart()

// Function that controls when which labels should be displayed
function labelLevels(d) {
  // Position all the parent labels in the middle of the scale
  if (typeof d.parent != 'undefined') {
    var depth = d.depth,
        loc = "d.parent";
    for (var i=0; i<depth;i++) {
      var parent = eval(loc).nam;
      d3.selectAll('.abbr').filter(function(d, i) {
        return d.nam === parent;
      }).attr("x", 430);
      d3.selectAll('.fullName').filter(function(d, i) {
        return d.nam === parent;
      }).attr("x", 430);
      loc += ".parent";
    }
    d3.selectAll('.abbr').filter(function(d, i) {
      return d.nam === parent;
    }).attr("x", 430);
    d3.selectAll('.fullName').filter(function(d, i) {
      return d.nam === parent;
    }).attr("x", 430);
  }
  //console.log(d);
  switch (d.lvl) {
    case 'undefined': 
      d3.selectAll('.level1').style("display", "block");
      d3.selectAll('.level2, .level3, .level4, .level5').style("display", "none");
      d3.selectAll('.abbr.level1').style("display", "none");
      d3.selectAll('.abbr.level2').style("display", "block");
      break;
    case 1:
      d3.selectAll('.level1, .level2').style("display", "block");
      d3.selectAll('.level3, .level4, .level5').style("display", "none");
      d3.selectAll('.abbr.level3').style("display", "block");
      d3.selectAll('.abbr.level1, .abbr.level2').style("display", "none");
      d3.selectAll('.abbr').filter(function(d, i) {
        return d.abr === "Q";
      }).style("display", "none");
      break;
    case 2:
      d3.selectAll('.level1, .level2, .level3').style("display", "block");
      d3.selectAll('.level4, .level5').style("display", "none");
      d3.selectAll('.abbr.level4').style("display", "block");
      d3.selectAll('.abbr.level1, .abbr.level2, .abbr.level3').style("display", "none");

      d3.selectAll('.fullName').filter(function(d, i) {
        return d.nam === "Quaternary";
      }).style("display", "none");

      d3.selectAll('.abbr').filter(function(d, i) {
        return d.abr === "Q";
      }).style("display", "block");

      d3.selectAll('.abbr').filter(function(d, i) {
        return d.abr === "H";
      }).style("display", "none");
      break;
    case 3:
      d3.selectAll('.level1, .level2, .level3, .level4').style("display", "block");
      d3.selectAll('.level5').style("display", "none");
      d3.selectAll('.abbr.level5').style("display", "block");
      d3.selectAll('.abbr.level4, .abbr.level3, .abbr.level2').style("display", "none");

      d3.selectAll('.fullName').filter(function(d, i) {
        return d.nam === "Holocene";
      }).style("display", "none");

      d3.selectAll('.abbr').filter(function(d, i) {
        return d.abr === "H";
      }).style("display", "block");
      break;
    case 4:
      d3.selectAll('.level1, .level2, .level3, .level4, .level5').style("display", "block");
      d3.selectAll('.abbr').style("display", "none");
      if (d.nam != "Holocene") {
        d3.selectAll('.fullName').filter(function(d, i) {
          return d.nam === "Holocene";
        }).style("display", "none");

        d3.selectAll('.abbr').filter(function(d, i) {
          return d.abr === "H";
        }).style("display", "block");
      }
      break;
    case 5:
      d3.selectAll('.level1, .level2, .level3, .level4, .level5').style("display", "block");
      d3.selectAll('.abbr').style("display", "none");
      break;
    default:
      d3.selectAll('.level1').style("display", "block");
      d3.selectAll('.level2, .level3, .level4, .level5').style("display", "none");
      d3.selectAll('.abbr.level1').style("display", "none");
      d3.selectAll('.abbr.level2').style("display", "block");
      break;
  }
} // end labelLevels()

function labelAbbrX(d) {
  var xPos = parseFloat(d3.select("#t" + d.oid).attr("width"))/2 + parseFloat(d3.select("#t" + d.oid).attr("x"));
  switch(d.nam) {
    case "Eoarchean":
      return xPos - 5;
      break;
    case "Paleoarchean":
      return xPos - 2;
      break;
    case "Mesoarchean":
      return xPos - 5;
      break;
    case "Neoarchean":
      return xPos - 3;
      break;
    case "Paleozoic":
      return xPos - 4;
      break;
    case "Cenozoic":
      return xPos - 8;
      break;
    case "Mesozoic":
      return xPos - 5;
      break;
    case "Cambrian":
      return xPos - 6;
      break;
    case "Ordovician":
      return xPos - 3;
      break;
    case "Silurian":
      return xPos - 4;
      break;
    case "Devonian":
      return xPos - 5;
      break;
    case "Series 2":
      return xPos - 3;
      break;
    case "Series 3":
      return xPos - 3;
      break;
    case "Furongian":
      return xPos - 3;
      break;
    case "Early Ordovician":
      return xPos - 3;
      break;
    case "Middle Ordovician":
      return xPos - 5;
      break;
    case "Late Ordovician":
      return xPos - 4;
      break;
    case "Hirnantian":
      return xPos - 5;
      break;
    case "Llandovery":
      return xPos - 3;
      break;
    case "Wenlock":
      return xPos - 6;
      break;
    case "Ludlow":
      return xPos - 4;
      break;
    case "Pridoli":
      return xPos - 5;
      break;
    case "Kasimovian":
      return xPos - 4;
      break;
    case "Gzhelian":
      return xPos - 5;
      break;
    case "Guadalupian":
      return xPos - 5;
      break;
    case "Lopingian":
      return xPos - 3;
      break;
    case "Middle Devonian":
      return xPos - 6;
      break;
    case "Triassic":
      return xPos - 5;
      break;
    case "Early Triassic":
      return xPos - 5;
      break;
    case "Middle Triassic":
      return xPos - 5;
      break;
    case "Late Triassic":
      return xPos - 5;
      break;
    case "Middle Jurassic":
      return xPos - 5;
      break;
    case "Hettangian":
      return xPos - 5;
      break;
    case "Aalenian":
      return xPos - 3;
      break;
    case "Bajocian":
      return xPos - 4;
      break;
    case "Bathonian":
      return xPos - 4;
      break;
    case "Callovian":
      return xPos - 5;
      break;
    case "Berriasian":
      return xPos - 3;
      break;
    case "Valanginian":
      return xPos - 2;
      break;
    case "Hauterivian":
      return xPos - 4;
      break;
    case "Barremian":
      return xPos - 3;
      break;
    case "Aptian":
      return xPos - 5;
      break;
    case "Albian":
      return xPos - 5;
      break;
    case "Cenomanian":
      return xPos - 3;
      break;
    case "Turonian":
      return xPos - 3;
      break;
    case "Coniacian":
      return xPos - 4;
      break;
    case "Santonian":
      return xPos - 4;
      break;
    case "Campanian":
      return xPos - 3;
      break;
    case "Maastrichtian":
      return xPos - 5;
      break;
    case "Neogene":
      return xPos - 8;
      break;
    case "Quaternary":
      return xPos - 6;
      break;
    case "Holocene":
      return xPos - 7;
      break;
    case "Bartonian":
      return xPos - 3;
      break;
    case "Priabonian":
      return xPos - 3;
      break;
    default:
      return xPos;
      break;
  }
} // end labelAbbrX()
function labelX(d) {
  var xPos = parseFloat(d3.select("#t" + d.oid).attr("width"))/2 + parseFloat(d3.select("#t" + d.oid).attr("x"));
  switch (d.nam) {
    case "Phanerozoic":
      return xPos - 35;
      break;
    case "Paleozoic":
      return xPos - 35;
      break;
    case "Cambrian":
      return xPos - 25;
      break;
    case "Terreneuvian":
      return xPos - 40;
      break;
    case "Series 2":
      return xPos - 25;
      break;
    case "Series 3":
      return xPos - 25;
      break;
    case "Furongian": 
      return xPos - 30;
      break;
    case "Ordovician":
      return xPos - 34;
      break;
    case "Silurian":
      return xPos - 25;
      break;
    case "Devonian":
      return xPos - 30;
      break;
    case "Carboniferous":
      return xPos - 44;
      break;
    case "Permian":
      return xPos - 24;
      break;
    case "Cenozoic":
      return xPos - 30;
      break;
    case "Middle Ordovician":
      return xPos - 67;
      break;
    case "Late Ordovician":
      return xPos - 55;
      break;
    case "Late Ordovician":
      return xPos - 39;
      break;
    case "Wenlock":
      return xPos - 33;
      break;
    case "Ludlow":
      return xPos - 25;
      break;
    case "Pridoli":
      return xPos - 20;
      break;
    case "Pragian":
      return xPos - 25;
      break;
    case "Kasimovian":
      return xPos - 35;
      break;
    case "Gzhelian":
      return xPos - 30;
      break;
    case "Moscovian":
      return xPos - 35;
      break;
    case "Bashkirian":
      return xPos - 30;
      break;
    case "Lopingian":
      return xPos - 32;
      break;
    case "Asselian":
      return xPos - 25;
      break;
    case "Sakmarian":
      return xPos - 32;
      break;
    case "Kungurian":
      return xPos - 35;
      break;
    case "Triassic":
      return xPos - 25;
      break;
    case "Early Triassic":
      return xPos - 42;
      break;
    case "Jurassic":
      return xPos - 20;
      break;
    case "Hettangian":
      return xPos - 35;
      break;
    case "Aalenian":
      return xPos - 25;
      break;
    case "Bajocian":
      return xPos - 25;
      break;
    case "Bathonian":
      return xPos - 30;
      break;
    case "Callovian":
      return xPos - 30;
      break;
    case "Berriasian":
      return xPos - 32;
      break;
    case "Valanginian":
      return xPos - 33;
      break;
    case "Hauterivian":
      return xPos - 36;
      break;
    case "Barremian":
      return xPos - 32;
      break;
    case "Aptian":
      return xPos - 25;
      break;
    case "Albian":
      return xPos - 25;
      break;
    case "Cenomanian":
      return xPos - 35;
      break;
    case "Turonian":
      return xPos - 25;
      break;
    case "Coniacian":
      return xPos - 30;
      break;
    case "Santonian":
      return xPos - 32;
      break;
    case "Campanian":
      return xPos - 40;
      break;
    case "Maastrichtian":
      return xPos - 35;
      break;
    case "Cretaceous":
      return xPos - 40;
      break;
    case "Bartonian":
      return xPos - 27;
      break;
    case "Priabonian":
      return xPos - 30;
      break;
    case "Lutetian":
      return xPos - 25;
      break;
    case "Ypresian":
      return xPos - 30;
      break;
    case "Gelasian":
      return xPos - 30;
      break;
    case "Middle":
      return xPos - 25;
      break;
    case "Late":
      return xPos -12;
      break;
    default: 
      return xPos - 50;
  }
} // end labelX()

// Function to handle the zooming action
function rectTrans(d) {
  // if box is checked...
  // if box is checked...
  var timeFilterCheck = document.getElementById('viewByTimeBox').checked;
  if (timeFilterCheck) {
    selectedInterval = d.nam;
    refresh(d);
  }

  var reconstructCheck = document.getElementById('reconstructBox').checked;
  if (reconstructCheck){
    var requestYear = parseInt((d.eag + d.lag) / 2);
    if (d.depth < 3) {
      return alert("Please select a period or finer interval");
    } else if (requestYear > 250) {
      return alert("Please select an interval younger than 250 MA");
    } else {
      reconstruct(d);
    }
  }

  // var n keeps track of the transition
  var n = 0,
    w = 960,
    h = 120,
    x = d3.scale.linear().range([0, w]),
    y = d3.scale.linear().range([0, h]),
    rect = d3.selectAll("rect");

  x.domain([d.x, d.x + d.dx]);

  // When complete, calls labelTrans() 
  rect.transition()
  .duration(750)
  .each(function(){ ++n; })
  .attr("x", function(d) { return x(d.x); })
  .attr("width", function(d) { return x(d.x + d.dx) - x(d.x); })
  .each("end", function() { if (!--n) labelTrans(d)});
}

// Function that handles label placement while zooming
// Depends on the result of rectTrans(), so executed only after rectTrans() is complete
function labelTrans(d) {
  // var n keeps track of the transition
  console.log(d);
  var n = 0,
    w = 960,
    h = 120,
    x = d3.scale.linear().range([0, w]),
    y = d3.scale.linear().range([0, h]),
    labelsFull = d3.selectAll(".fullName"),
    labelsAbbr = d3.selectAll(".abbr");

  x.domain([d.x, d.x + d.dx]);

  // Move the full names
  labelsFull.transition()
  .duration(300)
  .each(function(){ ++n; })
  .attr("x", function(d) { return labelX(d); })
  .attr("width", function(d) { return 120; })
  .attr("height", function(d) { return y(d.y + d.dy) - y(d.y); })
  .each("end", function() { if (!--n) labelLevels(d)});

  // Move the abbreviations
  labelsAbbr.transition()
  .duration(300)
  .each(function(){ ++n; })
  .attr("x", function(d) { return labelAbbrX(d); })
  .attr("width", function(d) { return 23;})
  .attr("height", function(d) { return y(d.y + d.dy) - y(d.y); })
  .each("end", function() { if (!--n) labelLevels(d)});

  var current = d3.selectAll('.fullName').filter(function(e, i) {
      return e.nam == d.nam;
    }).insert("i").attr("class", "icon-globe icon2x");
  //.append('<i class="icon-globe icon2x"></i>');
}
function updateTaxon(id) {
  if (!id) {
    taxon = '';
    exclude = '';
    refresh();
  } else {
    taxon = {oid: id};
    refresh();
  }
}

function excludeTaxon(id) {
  console.log(exclude);
  if (!id) {
    exclude = '';
    refresh();
  } else {
    exclude = {oid: id};
    console.log(exclude);
    refresh();
  }
  
}
function sizeChange() {
  d3.select("#map")
    .style("height", function(d) {
      return window.innerHeight * 0.50 + "px";
    });
  map.invalidateSize();

  d3.select("#reconstructMap")
    .style("height", function(d) {
      return window.innerHeight * 0.50 + "px";
    });
  reconstructMap.invalidateSize();

  d3.select("#window")
    .style("height", function(d) {
      return parseInt(d3.select("#map").style("height")) - 16 + "px";
    });

  d3.select("#chartGroup")
    .attr("transform", function(d) {
      return "scale(" + parseInt(d3.select("#chart").style("width"))/961 + ")";
    });

  d3.select("#geologicTime")
    .style("width", function(d) {
      return d3.select("#chart").style("width");
     })
    .style("height", function(d) {
      return parseInt(d3.select("#chart").style("width")) * 0.20 + "px";
    });
}

function point() {
  mapType = "points";
  return refresh();
}
function heat() {
  mapType = "heat";
  return refresh();
}

function filterByPerson(name) {
  /*if (name) {
    if (name == 'none') {
      personFilter = '';
      refresh();
      return false;
    }
    var person = name;
    d3.json('http://testpaleodb.geology.wisc.edu/data1.1/people/list.json?name=' + person, function(err, result) {
      if (err || result.records.length < 1) {
        return alert("No people with that name found. Please try again.");
      }
      personFilter = result.records[0].oid;
      console.log("Filtering by ", personFilter);
      refresh();
      return false;
    });
  }*/
  var checked = document.getElementById("personFilterBox").checked;
  if (checked == true) {
    var person = d3.select("#personInput").property("value");
    d3.json('http://testpaleodb.geology.wisc.edu/data1.1/people/list.json?name=' + person, function(err, result) {
      if (err || result.records.length < 1) {
        return alert("No people with that name found. Please try again.");
      }
      personFilter = result.records[0].oid;
      console.log("Filtering by ", personFilter);
      refresh();
      return false;
    });
  } else {
    personFilter = '';
    console.log("not checked");
    return false;
  }
  return false;
}

// Initialize the map and time scale on load
(function() {
  // Set the initial map type
  mapType = "points";

  // Init the leaflet map
  map = new L.Map('map', {
    center: new L.LatLng(7, 0),
    zoom: 2,
    maxZoom:10,
    minZoom: 2,
    zoomControl: false
  });

  reconstructMap = new L.Map('reconstructMap', {
    center: new L.LatLng(7, 0),
    zoom: 2,
    maxZoom:10,
    minZoom: 2,
    zoomControl: false
  });

  var reconstructMapControl = L.control({position: 'topleft'});
  reconstructMapControl.onAdd = function(map) {
    var div = L.DomUtil.create('div', 'customControl');
    div.innerHTML += "<div id='reconstructMapInfo'><a href='http://gplates.org' target='_blank'><img id='gplatesLogo' src='css/gplates_icon.jpg'></a><h4 id='age'></h4><p>via GPlates</p></div><a id='mapSwitch' href='#''><i class=icon-circle-arrow-left icon-2x></i> Back to main map</a>";
    return div;
  }
  reconstructMapControl.addTo(reconstructMap);

  var attrib = 'Map tiles by <a href="http://stamen.com">Stamen Design</a>, <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a> &mdash; Map data © <a href="http://openstreetmap.org">OpenStreetMap</a> contributors';

  stamen = new L.TileLayer('http://{s}.tile.stamen.com/toner-background/{z}/{x}/{y}.png', {attribution: attrib}).addTo(map);

  stamenLabels = new L.TileLayer('http://{s}.tile.stamen.com/toner/{z}/{x}/{y}.png', {attribution: attrib});

  map.on("moveend", function() {
    d3.select("#window").style("display", "none");
    d3.select(".layerToggler").style("display", "none");
    d3.selectAll("rect").style("stroke", "#fff");
    d3.select(".info")
      .style("display", "none");

    refresh();
    
  });

  var credits = new L.control({position: 'bottomleft'});
  credits.onAdd = function(map) {
    var div = L.DomUtil.create('div', 'info credits');
    return div;
  }
  credits.addTo(map);

  // Add a div to house mouseover data
  var info = L.control({position: 'bottomleft'});
  info.onAdd = function (map) {
    var div = L.DomUtil.create('div', 'info');
    return div;
  }
  info.addTo(map);

  var controlContent = '<div class="zoom-in">+</div><div class="zoom-out">&#8211;</div><div class="layers"></div><div class="tools"><i class="icon-wrench icon-2x"></i></div><div class="userFilter"><i class="icon-user icon-2x"></i></div>';

  var controls = L.control({position: 'topleft'});
  controls.onAdd = function(map) {
  var div = L.DomUtil.create('div', 'customControl buttons');
  div.innerHTML += controlContent;
  return div;
  }
  controls.addTo(map);

  var layerToggler = L.control({position:'topleft'});
  layerToggler.onAdd = function(map) {
  var div = L.DomUtil.create('div', 'customControl layerToggler');
  div.innerHTML += '<form action=""><input type="radio" name="layer" value="point" onclick="point();" checked>     Point<br><input type="radio" name="layer" value="heat" onclick="heat();">     Heat</form>';
  return div;
  }
  layerToggler.addTo(map);

  var toolToggler = L.control({position:'topleft'});
  toolToggler.onAdd = function(map) {
  var div = L.DomUtil.create('div', 'customControl toolToggler');
  div.innerHTML += '<form action=""><input id="viewByTimeBox" type="checkbox" value="1"/>Filter by time<br><input id="reconstructBox" type="checkbox" value="1"/>Rotate view by interval</form>';
  return div;
  }
  toolToggler.addTo(map);

  var userToggler = L.control({position:'topleft'});
  userToggler.onAdd = function(map) {
  var div = L.DomUtil.create('div', 'customControl userToggler');
  div.innerHTML += '<form onSubmit="return filterByPerson()"><input id="personFilterBox" type="checkbox" value="1"/>Filter by person<br><input type="text" id="personInput"/><button type="submit" class="btn btn-success btn-xs">Filter</button></form>';
  return div;
  }
  userToggler.addTo(map);

  var taxaToggler = L.control({position: 'topright'});
  taxaToggler.onAdd = function(map) {
    var div = L.DomUtil.create('div', 'customControl taxaToggler');
    div.innerHTML += '<div><i class="icon-bug icon-2x"></i></div>';
    return div;
  }
  taxaToggler.addTo(map);

  // attach handlers for layer toggler
  d3.select(".layers")
    .on("click", function(d) {
      var visible = d3.select(".layerToggler").style("display");
      if (visible == "block") {
        d3.select(".layerToggler").style("display", "none");
      } else {
        d3.select(".layerToggler").style("display", "block");
      }
    });

  // Attach handlers for zoom-in and zoom-out buttons
  d3.select(".zoom-in")
    .on("click",function() {
      map.zoomIn();
    });
  d3.select(".zoom-out")
    .on("click",function() {
      map.zoomOut();
    });

  d3.select("#mapSwitch")
    .on("click", function() {
      d3.select("#map").style("display", "block");
      d3.select("#reconstructMap").style("display","none");
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

  d3.select("#viewByTimeBox")
    .on("click", function() {
      var checked = document.getElementById("viewByTimeBox").checked;
      if (checked == true) {
        console.log("checked");
      } else {
        console.log("not checked");
        selectedInterval = '';
        refresh();
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
      if (checked == true) {
        // something
      } else {
        personFilter = '';
        refresh();
      }
    });

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

  buildTimeChart();
  refresh();
  setTimeout(sizeChange, 100);
  setTimeout(sizeChange, 100);
})();