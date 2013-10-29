function point() {
  mapType = "points";
  return refresh();
}
function heat() {
  mapType = "heat";
  return refresh();
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
// Clear the heatmap, if it exists
  if (Object.keys(map._layers).length > 1) {
    for(i in map._layers) {
      if(map._layers[i]._path != undefined) {
        map.removeLayer(map._layers[i]);
      }
    }
  }

 
function reconstruct(year) {

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

  d3.select('#age').text(requestYear + " Ma");
 
  if (zoom < 5) {
    var type = 'clusters';
    var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=1&limit=9999999&interval=' + year.nam;
    if (typeof filters.personFilter != 'undefined') {
      url += '&person_no=' + filters.personFilter;
    }
    if (typeof taxon != 'undefined') {
      url += '&base_id=' + taxon.oid;
    }
  } else if (zoom > 4 && zoom < 8 ) {
    var type = 'clusters';
    var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=2&limit=999999&interval=' + year.nam;
    if (typeof filters.personFilter != 'undefined') {
      url += '&person_no=' + filters.personFilter;
    }
    if (typeof taxon != 'undefined') {
      url += '&base_id=' + taxon.oid;
    }
  } else {
    var url = 'http://testpaleodb.geology.wisc.edu/data1.1/colls/list.json?lngmin=' + sw.lng + '&lngmax='
       + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&max_ma=' + year.eag + '&min_ma=' + year.lag + '&limit=999999';
    if (typeof filters.personFilter != 'undefined') {
      url += '&person_no=' + filters.personFilter;
    }
    if (typeof taxon != 'undefined') {
      url += '&base_id=' + taxon.oid;
    }
  }

  var keepers = [];

  d3.json(url, function(error, response) {
    console.log("first req made");

    var gPlatesReqData = '&time=' + requestYear + '&points="' + buildWKT(response.records) + '"&output=topojson';

    //TODO: Listen to this event and add a loader 
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

      d3.json("plates/plate" + requestYear + ".json", function(er, topoPlates) {
        console.log("plates loaded");
        var geojsonPlates = topojson.feature(topoPlates, topoPlates.objects["plates" + requestYear]);
        d3.select("#map").style("display", "none");
        d3.select("#reconstructMap").style("display","block");
        var plateLayer = L.geoJson(geojsonPlates, {
          style: {
            "color": "#000",
            "fillColor": "#FEFBFB",
            "weight": 1,
            "fillOpacity": 1
          }
        }).addTo(reconstructMap);

        var pointLayer = L.geoJson(rotatedPoints, {
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

        reconstructMap.invalidateSize();

        // Uncheck the checkbox
        document.getElementById("reconstructBox").checked = false;
        d3.select(".rotate").style("background-color", "");
        document.getElementById("viewByTimeBox").checked = false;
        d3.select(".time").style("background-color", "");
        
      });
    });
  });
    //var gPlatesReqData = '&time=' + requestYear + '&points="' + buildWKT(response.records) + '"&output=topojson';

    //TODO: Listen to this event and add a loader 
   /* d3.xhr('http://gplates.gps.caltech.edu:8080/recon_points/')
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

        rotatedPoints.features.forEach(function(d) {
          for (var i=0;i<response.records.length;i++) {
            if (parseInt(d.properties.NAME) == response.records[i].oid) {
              d.properties.nco = response.records[i].nco;
              d.properties.noc = response.records[i].noc;
              d.properties.cxi = response.records[i].cxi;
            }
          }
        });*/

      
      
 //   });

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
function updateDownloadLink(url, zoom) {
  d3.select("#download")
    .on("click", function() {
      if (zoom > 6) {
        // hacky mchackerson
        var link = url.replace("summary.json", "list.txt");
        link = link.replace("list.json", "list.txt");
        link = link.replace("&level=1", "");
        link = link.replace("&level=2", "");
        window.open(link);
      } else {
        alert("Please zoom in to the individual collection level and try downloading again");
      }
    });
}