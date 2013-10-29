// Global variables
var map, stamen, stamenLabels, g, exlude,
  prevsw = {"lng": 0, "lat": 0},
  prevne = {"lng": 0, "lat": 0},
  prevzoom = 3,
  filters = {"selectedInterval": {"nam": "", "mid": ""}, "personFilter": {"id":"", "name": ""}, "taxon": {"id": "", "name": ""}, "exist": {"selectedInterval" : false, "personFilter": false, "taxon": false}};

var navMap = {
  "init": function() {
    // Init the leaflet map
    map = new L.Map('map', {
      center: new L.LatLng(7, 0),
      zoom: 2,
      maxZoom:10,
      minZoom: 2,
      zoomControl: false,
      inertiaDeceleration: 6000,
      inertiaMaxSpeed: 1000,
      zoomAnimationThreshold: 1
    });

    var attrib = 'Map tiles by <a href="http://stamen.com">Stamen Design</a>, <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a> &mdash; Map data © <a href="http://openstreetmap.org">OpenStreetMap</a> contributors';

    stamen = new L.TileLayer('http://{s}.tile.stamen.com/toner-background/{z}/{x}/{y}.png', {attribution: attrib}).addTo(map);

    stamenLabels = new L.TileLayer('http://{s}.tile.stamen.com/toner/{z}/{x}/{y}.png', {attribution: attrib});

    map.on("moveend", function(event) {
      if (event.hard) {
        return;
      }
      d3.select(".info").style("display", "none");

      var zoom = map.getZoom();
      if (zoom < 3) {
        mapHeight = d3.select("#map").style("height");
        d3.select("#map").style("height", 0);
        d3.select("#svgMap").style("display", "block");
        navMap.resizeSvgMap();
       // d3.selectAll("path").attr("d", path);
      }
      navMap.refresh();
    });

    d3.select("#map").style("height", 0);

    // Get map ready for an SVG layer
    map._initPathRoot();

    // Add the SVG to hold markers to the map
    d3.select("#map").select("svg")
      .append("g")
      .attr("id", "binHolder");

    function changeMaps(mouse) {
      var coords = mouse,
        projected = mercator.invert(coords);

      d3.select("#svgMap").style("display", "none");
      d3.select("#map").style("height", function() {
        return ((window.innerHeight * 0.70) - 70) + "px";
      });
      map.invalidateSize();

      map.setView([parseInt(projected[1]), parseInt(projected[0])], 3, {animate:false});
    }

    var width = 960,
        height = 500;
    
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

    var zoom = d3.behavior.zoom()
      .on("zoom",function() {
        if (d3.event.sourceEvent.wheelDelta > 0) {
          changeMaps(d3.mouse(this));
        } else if (d3.event.sourceEvent.type == "touchmove") {
          changeMaps([d3.event.sourceEvent.pageX, d3.event.sourceEvent.pageY]);
        }
      });

    var hammer = d3.select("#svgMap").append("svg")
      .attr("width", width)
      .attr("height", height)
      .call(zoom)
      .on("click", function() {
        changeMaps(d3.mouse(this));
      })
      .append("g");

    hammer.append("defs").append("path")
      .datum({type: "Sphere"})
      .attr("id", "sphere")
      .attr("d", path);

    hammer.append("use")
      .attr("class", "fill")
      .attr("xlink:href", "#sphere");

    d3.json("js/countries_1e5.json", function(error, data) {
      hammer.append("path")
        .datum(topojson.feature(data, data.objects.countries))
        .attr("class", "countries")
        .attr("d", path);

      reconstructMap.resize();
      timeScale.resize();

      navMap.refresh("reset");
      navMap.resizeSvgMap();
      setTimeout(navMap.resize, 100);
      setTimeout(navMap.resize, 100);
      navMap.resizeSvgMap();
    });
   
    // Attach handlers for zoom-in and zoom-out buttons
    d3.select(".zoom-in").on("click", function() {
      if (parseInt(d3.select("#map").style("height")) < 1) {
        d3.event.stopPropagation();
        d3.select("#svgMap").style("display", "none");
        d3.select("#map").style("height", function() {
          return d3.select("#svgMap").select("svg").style("height");
        });
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

    d3.select(".time")
      .on("click", function() {
        var checked = document.getElementById("viewByTimeBox").checked;
        if (checked == true) {
          document.getElementById("viewByTimeBox").checked = false;
          d3.select(".time")
            .style("color", "#000");

          d3.select(".info")
            .html("")
            .style("display", "none");

          if (document.getElementById("reconstructBox").checked) {
            document.getElementById("reconstructBox").checked = false;
            d3.select(".rotate")
              .style("color", "#000");
          }

        } else {
          document.getElementById("viewByTimeBox").checked = true;
          d3.select(".time")
            .style("color", "#ff992c");

          d3.select(".info")
            .html("Click a time interval to filter map")
            .style("display", "block");
        }
      });

    d3.select(".rotate")
      .on("click", function() {
        var rotateChecked = document.getElementById("reconstructBox").checked,
            timeChecked = document.getElementById("viewByTimeBox").checked;
        if (rotateChecked == true) {
          document.getElementById("reconstructBox").checked = false;
          document.getElementById("viewByTimeBox").checked = false;

          d3.select(".rotate")
            .style("box-shadow", "")
            .style("color", "#000");

          d3.select(".time")
            .style("color", "#000");

          d3.select(".info")
            .html("")
            .style("display", "none");

          navMap.refresh("reset");

          var rotateMapDisplay = d3.select("#reconstructMap").style("display");
          if (rotateMapDisplay == "block") {
            d3.select("#reconstructMap").style("display","none");
            timeScale.unhighlight();
            d3.select("#mapControlCover").style("display", "none");

            /*if (navMap.checkFilters()) {
              d3.select(".filters").style("display", "block");
            }*/

            if(parseInt(d3.select("#map").style("height")) < 1) {
              d3.select("#svgMap").style("display", "block");
            } else {
              d3.select("#map").style("display", "block");
            }
          }

        } else {
          if (timeChecked == false) {
            document.getElementById("viewByTimeBox").checked = true;

          }
          navMap.untoggleTaxa();
          navMap.untoggleUser();
          navMap.closeTaxaBrowser();

          document.getElementById("reconstructBox").checked = true;
          d3.select(".rotate")
            .style("box-shadow", "inset 3px 0 0 #ff992c")
            .style("color", "#ff992c");

          d3.select(".info")
            .html("Click a time interval to reconstruct collections and plates")
            .style("display", "block");

          var rotateMapDisplay = d3.select("#reconstructMap").style("display");
          if (rotateMapDisplay == "none") {
            if(parseInt(d3.select("#map").style("height")) > 1) {
              d3.select("#map").style("display", "none");
            }
            d3.select("#svgMap").style("display", "none");
            d3.select("#reconstructMap").style("display","block");
            //d3.select(".filters").style("display", "none");
            reconstructMap.resize();
            d3.select("#mapControlCover").style("display", "block");

            if (filters.exist.selectedInterval) {
              reconstructMap.rotate(filters.selectedInterval);
            } else {
              if (currentReconstruction.length < 1) {
                alert("Please click a time interval below to build a reconstruction map");
              }
            }
          }
        }
      });
    
    d3.select(".taxa")
      .on("click", function() {
        var visible = d3.select(".taxaToggler").style("display");
        if (visible == "block") {
          navMap.untoggleTaxa();
        } else {
          var browserVisible = d3.select("#taxaBrowser").style("display");
          if (browserVisible == "block") {
            navMap.closeTaxaBrowser();
            navMap.untoggleTaxa();
          } else {
            navMap.untoggleUser();

            d3.select(".taxaToggler").style("display", "block");
            d3.select(".taxa")
                .style("color", "#ff992c");
            }
          
        }
      });

    d3.select(".userFilter")
      .on("click", function() {
        var visible = d3.select(".userToggler").style("display");
        if (visible == "block") {
          navMap.untoggleUser();
        } else {
          navMap.untoggleTaxa();

          d3.select(".userToggler").style("display", "block");
          d3.select(".userFilter")
              .style("color", "#ff992c");
        }
      });

    var typeahead = $("#personInput").typeahead({
      name: 'contribs',
      prefetch: {
        url: '/data1.1/people/list.json?name=%',
        filter: function(data) {
          return data.records;
        }
      },
      valueKey: 'nam',
      limit: 8
    });

    typeahead.on('typeahead:selected', function(evt, data) {
      navMap.filterByPerson(data);
    });

    //attach window resize listener to the window
    d3.select(window).on("resize", function() {
      timeScale.resize();
      navMap.resize();
      reconstructMap.resize();
    });

  },
  "goTo": function(coords, zoom) {
    // coords is [lat, lng] 
    if (zoom < 3) {
      return;
    } else {
      d3.select("#svgMap").style("display", "none");
      d3.select("#map").style("height", function() {
        return window.innerHeight * 0.70 + "px";
      });
      map.invalidateSize();

      map.setView(coords, zoom, {animate:false});
    }
  },
  "selectBaseMap": function(zoom) {
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
  },
  "refresh": function(reset) {
    navMap.showLoading();
    var filtered = navMap.checkFilters();
    // Check which map is displayed - if hammer, skip the rest
    if (parseInt(d3.select("#map").style("height")) < 1) {
      var url = '/data1.1/colls/summary.json?lngmin=-180&lngmax=180&latmin=-90&latmax=90&limit=999999';
      if (filtered) {
        if (filters.exist.selectedInterval == true && !filters.exist.personFilter && !filters.exist.taxon) {
          url = "collections/" + filters.selectedInterval.nam.split(' ').join('_') + ".json";
        } else {
          url += "&level=2";
          url = navMap.parseURL(url);
        }
      } else {
        url += "&level=1";
        url = navMap.parseURL(url);
      }

      d3.json(url, function(error, data) { 
        navMap.refreshHammer(data);
      });
      return;
    }

    var bounds = map.getBounds(),
      sw = bounds._southWest,
      ne = bounds._northEast,
      zoom = map.getZoom();

    if(!reset) {
      // Check if new points are needed from the server
      if (prevne.lat > ne.lat && prevne.lng > ne.lng && prevsw.lat < sw.lat && prevsw.lng < sw.lng) {
        if (prevzoom < 4 && zoom > 3) {
          // refresh
        } else if (prevzoom < 7 && zoom > 6) {
          //refresh
        } else {
          var points = d3.selectAll(".bins");
          if (zoom > 6) {
            var clusters = d3.selectAll(".clusters");
            return navMap.redrawPoints(points, clusters);
          } else {
            return navMap.redrawPoints(points);
          }
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
    navMap.selectBaseMap(zoom);

    // Remove old points
    d3.selectAll(".bins").remove();
    d3.selectAll(".clusters").remove();

    // Redefine to check if we are crossing the date line
    bounds = map.getBounds();

    // Depending on the zoom level, call a different service from PaleoDB, feed it a bounding box, and pass it to function getData
    if (zoom < 4 && filtered == false) {
      var url = '/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=1&limit=999999';

      d3.json(navMap.parseURL(url), function(error, data) {
        data.records.forEach(function(d) {
          d.LatLng = new L.LatLng(d.lat,d.lng)
        });
        if (bounds._southWest.lng < -180 || bounds._northEast.lng > 180) {
          navMap.refreshDateline(1, data);
        } else {
          navMap.drawBins(data, 1, zoom);
        }
      });
    } else if (zoom > 3 && zoom < 7 || zoom < 4 && filtered == true) {
      // TODO: like above, if only filtering by time load from static
      var url = '/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=2&limit=99999';

      if (bounds._southWest.lng < -180 || bounds._northEast.lng > 180) {
        navMap.refreshDateline(2);
      }
      d3.json(navMap.parseURL(url), function(error, data) {
        data.records.forEach(function(d) {
          d.LatLng = new L.LatLng(d.lat,d.lng)
        });
        navMap.drawBins(data, 2, zoom);
      });
    } else {
      var url = '/data1.1/colls/list.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&limit=99999999&show=time';

      if (bounds._southWest.lng < -180 || bounds._northEast.lng > 180) {
        navMap.refreshDateline(3);
      }
      d3.json(navMap.parseURL(url), function(error, data) {
        data.records.forEach(function(d) {
          d.LatLng = new L.LatLng(d.lat,d.lng)
        });
        navMap.drawCollections(data, 3, zoom);
      });
      
    }
  },
  // Adjust the positioning of the SVG elements relative to the map frame
  "redrawPoints": function(points, clusterPoints) {
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
      points.attr("r", function(d) { return scale(d.nco)*navMap.multiplier(zoom); });
    }
    
    if (d3.select("#reconstructMap").style("display") == "none") {
      navMap.hideLoading();
    }
  },
  "refreshHammer": function(data) {
    var scale = d3.scale.linear()
      .domain([1, 4240])
      .range([4, 15]);

    var width = 960,
        height = 500;

    var projection = d3.geo.hammer()
      .scale(165)
      .translate([width / 2, height / 2])
      .precision(.1);

    var path = d3.geo.path()
      .projection(projection);

    var hammer = d3.select("#svgMap").select("svg").select("g"),
        zoom = 2;

    var bins = hammer.selectAll(".bins")
      .data(data.records)
      .attr("class", "bins")
      .style("fill", function(d) { return (interval_hash[d.cxi]) ? interval_hash[d.cxi].col : "#000"; })
      .attr("id", function(d) { return "p" + d.cxi; })
      .attr("r", function(d) { return scale(d.nco)*navMap.multiplier(zoom); })
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
        navMap.openBinModal(d);
      })
      .on("mouseout", function(d) {
        d3.select(".info")
          .html("")
          .style("display", "none");
        timeScale.unhighlight()
      });

    bins.enter().append("circle")
      .attr("class", "bins")
      .style("fill", function(d) { return (interval_hash[d.cxi]) ? interval_hash[d.cxi].col : "#000"; })
      .attr("id", function(d) { return "p" + d.cxi; })
      .attr("r", function(d) { return scale(d.nco)*navMap.multiplier(zoom); })
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

    bins.exit().remove();
    if (!reconstructing) {
      navMap.hideLoading();
    }

  },
  "drawBins": function(data, level, zoom) {
    var g = d3.select("#binHolder");
    // Add the bins to the map
    var points = g.selectAll(".bins")
      .data(data.records)
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
        navMap.openBinModal(d);
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

    points.enter().append("circle")
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
        navMap.openBinModal(d);
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

    points.exit().remove();

    // Update the SVG positioning
    navMap.redrawPoints(points);
  },
  "drawCollections": function(data, level, zoom) {
    var g = d3.select("#binHolder");

    // Many collections share the same coordinates, making it necessary to create clusters of like coordinates
    var clusters = [];
    // For each collection, check it's coordinates against all others and see if any matches exist
    for (var i=0; i<data.records.length; i++) {
      for (var j=0; j<data.records.length; j++) {
        // If another collection has the same lat/lng and a different OID, create a new cluster
        // SIDENOTE: this could be extended for binning by specifying a tolerance instead of an exact match of coordinates
        if (data.records[i].lat == data.records[j].lat && data.records[i].lng == data.records[j].lng && data.records[i].oid != data.records[j].oid) {
          var newCluster = {"lat":data.records[i].lat, "lng":data.records[i].lng, "members": []},
              exists = 0;
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
      var index = navMap.arrayObjectIndexOf(data.records, toRemove[i], "oid");
      data.records.splice(index, 1);
    }
    
    // Create a Leaflet Lat/lng for all non-clustered collections
    data.records.forEach(function(d) {
      d.LatLng = new L.LatLng(d.lat,d.lng)
    });
    // Create a Leaflet Lat/lng for all clusters
    clusters.forEach(function(d) {
      d.LatLng = new L.LatLng(d.lat,d.lng);
      //var clusterBottoms = [],
      //  clusterTops = [],
      var totalOccurrences = [];

      d.members.forEach(function(e) {
        //clusterBottoms.push(e.eag);
        //clusterTops.push(e.lag);
        totalOccurrences.push(e.noc);
      });
      //d.ageTop = d3.min(clusterTops);
      //d.ageBottom = d3.max(clusterBottoms);
      // TODO: fix this to something more accurate
      /* Annecdotal evidence suggests all collections that share a lat/lng should be from the 
        same interval, but I doubt that it's always true */
      d.cxi = d.members[0].cxi;
      d.noc = d3.sum(totalOccurrences);
    });

    var clusters = g.selectAll(".clusters")
      .data(clusters)
      .attr("class", "clusters")
      .attr("id", function(d) { return "p" + d.members[0].cxi; })
      .style("fill", function(d) { return interval_hash[d.cxi].col; })
      .on("mouseover", function(d) {
        d3.select(".info")
          .html("<strong>" + d.members.length + " collections</strong><br>" + d.noc + " occurrences")
          .style("display", "block");
        timeScale.highlight(this);
      })
      .on("mouseout", function(d) {
       /* d3.select(".info")
          .html("")
          .style("display", "none");*/
        timeScale.unhighlight();
      })
      .on("click", function(d) {
        navMap.openStackedCollectionModal(d);
        /*d3.select("#clusterTable")
          .html("");

        d3.select("#window")
          .style("display", "block");

        d3.select("#windowCollapse")
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
              .html("<strong>" + e.nam + "</strong><br>" + e.noc + " occurrences")
              .style("display", "block");
            timeScale.highlight(e);
          })
          .on("mouseout", function(e) {
            timeScale.unhighlight();
          })
          .on("click", function(e) {
            d3.select(".info")
              .html("<strong>" + e.nam + "</strong><br>" + e.noc + " occurrences")
              .style("display", "block");
            navMap.openCollectionModal(e);
            timeScale.highlight(e);
          });*/
      });
    
    clusters.enter().append("circle")
      .attr("class", "clusters")
      .attr("id", function(d) { return "p" + d.members[0].cxi; })
      .style("fill", function(d) { return (interval_hash[d.cxi]) ? interval_hash[d.cxi].col : "#000"; })
      .on("mouseover", function(d) {
        d3.select(".info")
          .html("<strong>" + d.members.length + " collections</strong><br>" + d.noc + " occurrences")
          .style("display", "block");
        timeScale.highlight(this);
      })
      .on("mouseout", function(d) {
       /* d3.select(".info")
          .html("")
          .style("display", "none");*/
        timeScale.unhighlight();
      })
      .on("click", function(d) {
        navMap.openStackedCollectionModal(d);
       /* d3.select("#clusterTable")
          .html("");

        d3.select("#window")
          .style("display", "block");

        d3.select("#windowCollapse")
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
              .html("<strong>" + e.nam + "</strong><br>" + e.noc + " occurrences")
              .style("display", "block");
            timeScale.highlight(e);
          })
          .on("mouseout", function(e) {
            timeScale.unhighlight();
          })
          .on("click", function(e) {
            d3.select(".info")
              .html("<strong>" + e.nam + "</strong><br>" + e.noc + " occurrences")
              .style("display", "block");
            navMap.openCollectionModal(e);
            timeScale.highlight(e);
          });*/
      });
    
    clusters.exit().remove();

    var points = g.selectAll(".circle")
      .data(data.records)
      .attr("id", function(d) { return "p" + d.cxi })
      .attr("class", "bins")
      .style("fill", function(d) { return (interval_hash[d.cxi]) ? interval_hash[d.cxi].col : "#000"; })
      .on("mouseover", function(d) {
        d3.select(".info")
          .html("<strong>" + d.nam + "</strong><br>" + d.noc + " occurrences")
          .style("display", "block");
        timeScale.highlight(this);
      })
      .on("click", function(d) {
        d3.select(".info")
          .html("<strong>" + d.nam + "</strong><br>" + d.noc + " occurrences")
          .style("display", "block");
        timeScale.highlight(this);
        navMap.openCollectionModal(d);
      })
      .on("mouseout", function(d) {
        /*d3.select(".info")
          .html("")
          .style("display", "none");*/
        timeScale.unhighlight();
      });

    points.enter().append("circle")
      .attr("id", function(d) { return "p" + d.cxi })
      .attr("class", "bins")
      .style("fill", function(d) { return (interval_hash[d.cxi]) ? interval_hash[d.cxi].col : "#000"; })
      .on("mouseover", function(d) {
        d3.select(".info")
          .html("<strong>" + d.nam + "</strong><br>" + d.noc + " occurrences")
          .style("display", "block");
        timeScale.highlight(this);
      })
      .on("click", function(d) {
        d3.select(".info")
          .html("<strong>" + d.nam + "</strong><br>" + d.noc + " occurrences")
          .style("display", "block");
        timeScale.highlight(this);
        navMap.openCollectionModal(d);
      })
      .on("mouseout", function(d) {
        /*d3.select(".info")
          .html("")
          .style("display", "none");*/
        timeScale.unhighlight();
      });

    points.exit().remove();

    navMap.redrawPoints(points, clusters);

  },
  "openCollectionModal": function(d) {
    d3.json("/data1.1/colls/single.json?id=" + d.oid + "&show=ref", function(err, data) {

      data.records.forEach(function(d) {
        d.interval = (interval_hash[d.cxi]) ? interval_hash[d.cxi].nam : "Unknown";
        d.fmm = (d.fmm) ? d.fmm : "Unknown";
      });

      var template = '{{#records}}<table class="table"><tr><td style="border-top:0;"><strong>Collection number</strong></td><td style="border-top:0;">{{oid}}</td></tr><tr><td><strong>Occurrences</strong></td><td>{{noc}}</td></tr><tr><td><strong>Formation</strong></td><td>{{fmm}}</td></tr><tr><td><strong>Interval</strong></td><td>{{interval}}</td></tr><tr><td><strong>Location</strong><br><small>(latitude, longitude)</small></td><td>{{lat}}, {{lng}}</td></tr><tr><td><strong>Reference</strong></td><td>{{{ref}}}</td></tr></table>{{/records}}';

      var output = Mustache.render(template, data);
      $("#collectionName").html(data.records[0].nam);
      $("#collectionModalBody").html(output);
      $("#collectionBox").modal();
    });
  },
  "openBinModal": function(d) {
    var id = (d.properties) ? d.properties.oid : d.oid;
    var url = "/data1.1/colls/list.json?bin_id=" +id;
    url = navMap.parseURL(url);
    url += "&show=ref,loc,time"
    d3.json(url, function(err, data) {
      data.records.forEach(function(d) {
        d.interval = (interval_hash[d.cxi]) ? interval_hash[d.cxi].nam : "Unknown";
        d.fmm = (d.fmm) ? d.fmm : "Unknown";
      });

      var template = '{{#records}}<div class="panel panel-default"><a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion" href="#collapse{{oid}}"><div class="panel-heading"><p class="panel-title">{{nam}}</p></div></a><div id="collapse{{oid}}" class="panel-collapse collapse"><div class="panel-body"><table class="table"><tr><td style="border-top:0;"><strong>Collection number</strong></td><td style="border-top:0;">{{oid}}</td></tr><tr><td><strong>Occurrences</strong></td><td>{{noc}}</td></tr><tr><td><strong>Formation</strong></td><td>{{fmm}}</td></tr><tr><td><strong>Interval</strong></td><td>{{interval}}</td></tr><tr><td><strong>Location</strong><br><small>(latitude, longitude)</small></td><td>{{lat}}, {{lng}}</td></tr><tr><td><strong>Reference</strong></td><td>{{{ref}}}</td></tr></table></div></div></div>{{/records}}';

      var output = Mustache.render(template, data);
      d3.select("#binID").html("Bin " + id);
      d3.select("#accordion").html(output);

      $("#collectionModal").modal();
    });
  },
  "openStackedCollectionModal": function(data) {
    data.members.forEach(function(d) {
      d.interval = (interval_hash[d.cxi]) ? interval_hash[d.cxi].nam : "Unknown";
      d.fmm = (d.fmm) ? d.fmm : "Unknown";
    });

    var template = '{{#members}}<div class="panel panel-default"><a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion" href="#collapse{{oid}}"><div class="panel-heading"><p class="panel-title">{{nam}}</p></div></a><div id="collapse{{oid}}" class="panel-collapse collapse collectionCollapse"><div class="panel-body"><table class="table"><tr><td style="border-top:0;"><strong>Collection number</strong></td><td style="border-top:0;">{{oid}}</td></tr><tr><td><strong>Occurrences</strong></td><td>{{noc}}</td></tr><tr><td><strong>Formation</strong></td><td>{{fmm}}</td></tr><tr><td><strong>Interval</strong></td><td>{{interval}}</td></tr><tr><td><strong>Location</strong><br><small>(latitude, longitude)</small></td><td>{{lat}}, {{lng}}</td></tr><tr><td><strong>Reference</strong></td><td id="ref{{oid}}"></td></tr></table></div></div></div>{{/members}}';

    var output = Mustache.render(template, data);

    d3.select("#binID").html("Collections at [" + data.lat + ", " + data.lng + "]");
    d3.select("#accordion").html(output);

    $(".collectionCollapse").on("show.bs.collapse", function(d) {
      var id = d.target.id;
      id = id.replace("collapse", "");
      d3.json("/data1.1/colls/single.json?id=" + id + "&show=ref", function(err, data) {
        $("#ref" + id).html(data.records[0].ref);
      });
    });

    $("#collectionModal").modal();
  },
  "refreshDateline": function(lvl) {
    var bounds = map.getBounds(),
        sw = bounds._southWest,
        ne = bounds._northEast,
        zoom = map.getZoom(),
        west;

    sw.lng = (sw.lng < -180) ? sw.lng + 360 : sw.lng;
    sw.lat = (sw.lat < -90) ? -90 : sw.lat;
    ne.lng = (ne.lng > 180) ? ne.lng - 360 : ne.lng;
    ne.lat = (ne.lat > 90) ? 90 : ne.lat;

    bounds = map.getBounds();
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
        var url = '/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=1&limit=999999';
        url = navMap.parseURL(url);
        d3.json(url, function(error, response) {
          response.records.forEach(function(d) {
            if (west) {
              d.LatLng = new L.LatLng(d.lat,d.lng - 360);
            } else {
              d.LatLng = new L.LatLng(d.lat,d.lng + 360);
            }
          });
          navMap.drawBins(response, 1, zoom);
        });
        break;
      case 2:
        var url = '/data1.1/colls/summary.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&level=2&limit=99999';
        url = navMap.parseURL(url);
        d3.json(url, function(error, response) {
          response.records.forEach(function(d) {
            if (west) {
              d.LatLng = new L.LatLng(d.lat,d.lng - 360);
            } else {
              d.LatLng = new L.LatLng(d.lat,d.lng + 360);
            }
          });
          navMap.drawBins(response, 2, zoom);
        });
        break;
      case 3:
        var url = '/data1.1/colls/list.json?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&limit=99999999';
         url = navMap.parseURL(url);
         d3.json(url, function(error, response) {
          response.records.forEach(function(d) {
            if (west) {
              d.LatLng = new L.LatLng(d.lat,d.lng - 360);
            } else {
              d.LatLng = new L.LatLng(d.lat,d.lng + 360);
            }
          });
          navMap.drawCollections(response, 3, zoom);
        });
        //TODO add query and call appropriate function
        break;
    }
  },
  "buildWKT": function(data) {
    var requestString = "";
    for(var i=0; i<data.length; i++) {
      requestString += "POINT(" + data[i].lat + " " + data[i].lng + " " + data[i].oid + "),"
    }
    requestString = requestString.slice(0, -1);
    requestString = encodeURI(requestString);
    return requestString;
  },
  "parseURL": function(url) {
    var count = 0;
    for (key in filters.exist) {
      if (filters.exist.hasOwnProperty(key)) {
        if (filters.exist[key] == true) {
          switch(key) {
            case "selectedInterval":
              url += '&interval=' + filters.selectedInterval.nam;
              break;
            case "personFilter":
              url += '&person_no=' + filters.personFilter.id;
              break;
            case "taxon":
              url += '&base_id=' + filters.taxon.id;
              break;
          }
          count += 1;
        }
      }
    }
    if (count > 0 && d3.select("#reconstructMap").style("display") == "none") {
      d3.select(".filters").style("display", "block");
      /*d3.select(".filt")
        .style("box-shadow", "inset 3px 0 0 #ff992c")
        .style("color", "#ff992c");*/
    } else {
      //d3.select(".filters").style("display", "none");
      /*d3.select(".filt")
        .style("box-shadow", "")
        .style("color", "");*/
    }
    return url;
  },
  "checkFilters": function() {
    var count = 0;
    for (key in filters.exist) {
      if (filters.exist.hasOwnProperty(key)) {
        if (filters.exist[key] == true) {
          count += 1;
        }
      }
    }
    if (count > 0) {
      d3.select(".filters").style("display", "block");
      d3.select("#filterTitle").html("Filters");
      return true;
    } else {
      d3.select(".filters").style("display", "none");
      d3.select("#filterTitle").html("No filters selected");
      return false;
    }
  },
  "arrayObjectIndexOf": function(myArray, searchTerm, property) {
    for(var i=0, len=myArray.length; i<len; i++) {
      if (myArray[i][property] === searchTerm) return i;
    }
    return -1;
  },
  // Adjust the size of the markers depending on zoom level
  "multiplier": function(zoom) {
    switch(zoom) {
      case 2:
        return 0.70;
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
  },
  
  "resizeSvgMap": function() {
    var width = parseInt(d3.select("#graphics").style("width"));

    var g = d3.select("#svgMap").select("svg");

    d3.select("#svgMap").select("svg")
      .select("g")
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
        var height = ((window.innerHeight * 0.70) - 70);
        if (width > (box.width + 50)) {
          return "scale(" + window.innerHeight/800 + ")translate(" + ((width - box.width)/2) + ",0)";
        } else {
          var svgHeight = ((window.innerHeight * 0.70) - 70),
              mapHeight = (width/970 ) * 500;
          return "scale(" + width/970 + ")translate(0," + (svgHeight - mapHeight)/2 + ")";
        }

      });

    d3.select("#svgMap").select("svg")
      .style("height", function(d) {
        return ((window.innerHeight * 0.70) - 70) + "px";
      })
      .style("width", function(d) {
        return width - 15 + "px";
      });
  },

  "resize": function() {
    if (parseInt(d3.select("#map").style("height")) > 1) { 
      d3.select("#map")
        .style("height", function(d) {
          return ((window.innerHeight * 0.70) - 70) + "px";
        });
      map.invalidateSize();
    }

    navMap.resizeSvgMap();

    d3.select("#infoContainer")
      .style("height", function(d) {
        return ((window.innerHeight * 0.70) - 70) + "px";
      });

    d3.select(".filters")
      .style("bottom", function() {
        var height = parseInt(d3.select("#time").select("svg").style("height"));
        return (height + 20) + "px";
      });

  },
  "refreshFilterHandlers": function() {
    d3.selectAll(".removeFilter").on("click", function() {
      var parent = d3.select(this).node().parentNode;
      parent = d3.select(parent);
      parent.style("display", "none").html("");
      var type = parent.attr("id");
      filters.exist[type] = false;

      var keys = Object.keys(filters[type]);
      for (var i=0; i < keys.length; i++) {
        filters[type][keys[i]] = "";
      }

      if (d3.select("#reconstructMap").style("display") == "block") {
        reconstructMap.rotate(filters.selectedInterval);
      } else {
        navMap.refresh("reset");
      }

      switch(type) {
        case "selectedInterval":
          d3.select(".time").style("box-shadow", "");
          timeScale.unhighlight();
          break;
        case "personFilter":
          d3.select(".userFilter").style("box-shadow", "");
          break;
        case "taxon":
          d3.select(".taxa").style("box-shadow", "");
          break;
      }

    });
  },

  "updateFilterList": function(type) {

    switch(type){
      case "selectedInterval":
        d3.select("#selectedInterval")
          .style("display", "block")
          .html(filters.selectedInterval.nam + '<button type="button" class="close removeFilter" aria-hidden="true">&times;</button>');
        d3.select(".time").style("box-shadow", "inset 3px 0 0 #ff992c");
        navMap.refreshFilterHandlers();
        break;
      case "personFilter":
        d3.select("#personFilter")
          .style("display", "block")
          .html(filters.personFilter.name + '<button type="button" class="close removeFilter" aria-hidden="true">&times;</button>');
        d3.select(".userFilter").style("box-shadow", "inset 3px 0 0 #ff992c");
        navMap.refreshFilterHandlers();
        break;
      case "taxon":
        d3.select("#taxon")
          .style("display", "block")
          .html(filters.taxon.name + '<button type="button" class="close removeFilter" aria-hidden="true">&times;</button>');
        d3.select(".taxa").style("box-shadow", "inset 3px 0 0 #ff992c");
        navMap.refreshFilterHandlers();
        break;
    }
    
    d3.select(".filters")
      .style("bottom", function() {
        var height = parseInt(d3.select("#time").select("svg").style("height"));
        return (height + 20) + "px";
      });
  },

  "filterByTime": function(time) {
    // accepts a named time interval
    var d = d3.selectAll('rect').filter(function(e) {
      return e.nam === time;
    });
    d = d[0][0].__data__;
    filters.selectedInterval.nam = d.nam;
    filters.selectedInterval.mid = d.mid;
    filters.selectedInterval.col = d.col;
    filters.exist.selectedInterval = true;
    navMap.updateFilterList("selectedInterval");
  },

  "filterByTaxon": function(name) {
    if (!name) {
      var name = $("#taxaInput").val();
    }
    
    taxaBrowser.goToTaxon(name);

  },

  "filterByPerson": function(person, norefresh) {
    if (person) {
      filters.exist.personFilter = true;
      filters.personFilter.id = person.oid;
      filters.personFilter.name = (person.name) ? person.name : person.nam;
      navMap.updateFilterList("personFilter");
      d3.select(".userToggler").style("display", "none");
      d3.select(".userFilter")
          .style("color", "");

      if (d3.select("#reconstructMap").style("display") == "block") {
        reconstructMap.rotate(filters.selectedInterval);
      } else {
        navMap.refresh("reset");
      }
    }
  },

  "rotate":function(interval) {
    // interval is an object {nam: "interval", mid: 2342}
    document.getElementById("viewByTimeBox").checked = true;

    navMap.untoggleTaxa();
    navMap.closeTaxaBrowser();
    navMap.untoggleUser();

    document.getElementById("reconstructBox").checked = true;
    d3.select(".rotate")
      .style("box-shadow", "inset 3px 0 0 #ff992c")
      .style("color", "#ff992c");

    d3.select(".info")
      .html("Click a time interval to reconstruct collections and plates")
      .style("display", "block");

    var rotateMapDisplay = d3.select("#reconstructMap").style("display");
    if (rotateMapDisplay == "none") {
      if(parseInt(d3.select("#map").style("height")) > 1) {
        d3.select("#map").style("display", "none");
      }
      d3.select("#svgMap").style("display", "none");
      d3.select("#reconstructMap").style("display","block");
      //d3.select(".filters").style("display", "none");
      reconstructMap.resize();
      d3.select("#mapControlCover").style("display", "block");

      d3.selectAll(".ctrlButton")
        .style("color", "#777");

      reconstructMap.rotate(interval);

    }
  },

  "downloadView": function() {
    var bounds = map.getBounds(),
        sw = bounds._southWest,
        ne = bounds._northEast;

    if (parseInt(d3.select("#map").style("height")) < 1) {
      sw.lng = -180,
      ne.lng = 180,
      sw.lat = -90,
      ne.lat = 90;
    }

    var url = '/data1.1/colls/list.';

    if ($("#tsv:checked").length > 0) {
      url += "txt";
    } else {
      url += "csv";
    }
    url += '?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&limit=99999999';
    url = navMap.parseURL(url);

    var options = [];
    if ($("#loc:checked").length > 0) {
      options.push("loc");
    }
    if ($("#ref:checked").length > 0) {
      options.push("ref");
    }
    if ($("#t:checked").length > 0) {
      options.push("time");
    }
    if (options.length > 0) {
      url += "&show=";
      options.forEach(function(d) {
        url += d + ",";
      });
    }
    url = url.substring(0, url.length - 1);
    window.open(url);
  },

  "restoreState": function(state) {
    if (typeof state == "object") {
      var params = state;
      if (params.zoom > 2) {
        navMap.goTo(params.center, params.zoom);
      }
      if (params.timeScale != "Phanerozoic") {
        timeScale.goTo(params.timeScale);
      }
      if (params.taxonFilter.id > 0) {
        navMap.filterByTaxon(params.taxonFilter.nam);
      }
      if (typeof(params.timeFilter) == "object") {
        navMap.filterByTime(params.timeFilter.nam);
      }
      if (params.authFilter.id > 0) {
        navMap.filterByPerson(params.authFilter);
      }
      
      navMap.resize();
    }

    var location = window.location,
        state = location.hash.substr(2);

    // If there is a preserved state hash
    if (state.length > 1) {
      d3.json("/data1.1/...?key=" + state, function(error, result) {
        var params = result.records[0];

        if (params.timeScale != "Phanerozoic") {
          timeScale.goTo(params.timeScale);
        }
        if (params.taxonFilter.id > 0) {
          navMap.filterByTaxon(params.taxonFilter.nam);
        }
        if (params.timeFilter.length > 0) {
          navMap.filterByTime(params.timeFilter);
        }
        if (params.authFilter.id > 0) {
          navMap.filterByPerson(params.authFilter);
        }
        if (params.zoom > 2) {
          navMap.goTo(params.center, params.zoom);
        }
        if (reconstruct == "block") {
          navMap.rotate(params.currentReconstruction);
        }
      });
    } else {
      return;
    }
  },

  "getUrl": function() {
    //placeholder for generating a unique a unique hash
    var center = map.getCenter(),
        zoom = map.getZoom(),
        reconstruct = d3.select("#reconstructMap").style("display");

    var params = {"timeScale": currentInterval.nam, "taxonFilter": filters.taxon, "timeFilter": filters.selectedInterval, "authFilter": filters.personFilter, "zoom": zoom, "center": [center.lat, center.lng], "reconstruct": reconstruct, "currentReconstruction": currentReconstruction};
    
    return params;
  },

  "showLoading": function() {
    d3.select("#loading").style("display", "block");
  },

  "hideLoading": function() {
    d3.select("#loading").style("display", "none");
  },

  "untoggleTaxa": function() {
    d3.select(".taxaToggler").style("display", "none");
    d3.select(".taxa").style("color", "");
  },

  "untoggleUser": function() {
    d3.select(".userToggler").style("display", "none");
    d3.select(".userFilter").style("color", "");
  },

  "openTaxaBrowser": function() {
    d3.select("#graphics").attr("class", "col-sm-9");
    d3.select("#taxaBrowser").style("display", "block");
    d3.select("#taxaBrowserToggle").html('<i class="icon-double-angle-left" style="margin-right:5px;"></i>Collapse taxa browser');
    d3.select(".taxaToggler").style("display", "none");
    timeScale.resize();
    reconstructMap.resize();
    navMap.resize();
  },

  "closeTaxaBrowser": function() {
    d3.select("#graphics").attr("class", "col-sm-12");
    d3.select("#taxaBrowser").style("display", "none");
    d3.select("#taxaBrowserToggle").html('Expand taxa browser<i class="icon-double-angle-right" style="margin-left:5px;"></i>');
    d3.select(".taxa").style("color", "#000");
    timeScale.resize();
    reconstructMap.resize();
    navMap.resize();
    navMap.resize();
  }
}

timeScale.init("time");
navMap.init();
taxaBrowser.init();

$("#saveBox").on('show.bs.modal', function() {
  var count = 0;
  for (key in filters.exist) {
    if (filters.exist.hasOwnProperty(key)) {
      if (filters.exist[key] == true) {
        switch(key) {
          case "selectedInterval":
            $("#filterList").append("<li>Interval - " + filters.selectedInterval.nam + "</li>");
            break;
          case "personFilter":
            $("#filterList").append("<li>Contributor - " + filters.personFilter.name + "</li>");
            break;
          case "taxon":
            $("#filterList").append("<li>Taxon - " + filters.taxon.nam + "</li>");
            break;
        }
        count += 1;
      }
    }
  }
  if (count < 1) {
    $("#filterList").append("None selected");
  }
  var bounds = map.getBounds(),
      sw = bounds._southWest,
      ne = bounds._northEast;

  if (parseInt(d3.select("#map").style("height")) < 1) {
    sw.lng = -180,
    ne.lng = 180,
    sw.lat = -90,
    ne.lat = 90;
  }

  var url = '/data1.1/colls/list.json' + '?lngmin=' + sw.lng + '&lngmax=' + ne.lng + '&latmin=' + sw.lat + '&latmax=' + ne.lat + '&limit=0&count';
  url = navMap.parseURL(url);

  d3.json(url, function(err, results) {
    d3.select("#downloadCount").html(results.records_found + " collections found");
  });

});
$("#saveBox").on('hide.bs.modal', function() {
  $("#filterList").html('');
  $("#downloadCount").html("");
  $('#loc').prop('checked', false);
  $('#ref').prop('checked', false);
  $('#t').prop('checked', false);
});

$("#taxaForm").submit(function() {
  navMap.filterByTaxon();
  return false;
});

$(".taxaBrowserToggle").on("click", function() {
  var display = d3.select("#taxaBrowser").style("display");
  if (display == "block") {
    navMap.closeTaxaBrowser();
  } else {
    navMap.openTaxaBrowser();
  }
});
$("#trilobita").on("click", function() {
  var state = {
    "authFilter": {
      "id": "",
      "name": ""
    },
    "center": [30.3539163, 113.24707],
    "currentReconstruction": "",
    "reconstruct": "none",
    "taxonFilter": {
      "id": 19100,
      "nam": "Trilobita"
    },
    "timeFilter": {
      "nam": "Cambrian",
      "oid": 22,
      "mid": 513
    },
    "timeScale": "Cambrian",
    "zoom":5
  };
  navMap.restoreState(state);
});
$("#dinosauria").on("click", function() {
  var state = {
    "authFilter": {
      "id": "",
      "name": ""
    },
    "center": [40.5305, -109.5117],
    "currentReconstruction": "",
    "reconstruct": "none",
    "taxonFilter": {
      "id": 19968,
      "nam": "Dinosauria"
    },
    "timeFilter": {
      "nam": "Jurassic",
      "oid": 15,
      "mid": 173
    },
    "timeScale": "Jurassic",
    "zoom":5
  };
  navMap.restoreState(state);
});
$("#aves").on("click", function() {
  var state = {
    "authFilter": {
      "id": "",
      "name": ""
    },
    "center": [51.46085, 3.72436],
    "currentReconstruction": "",
    "reconstruct": "none",
    "taxonFilter": {
      "id": 98802,
      "nam": "Aves"
    },
    "timeFilter": {
      "nam": "Cenozoic",
      "oid": 1,
      "mid": 33
    },
    "timeScale": "Cenozoic",
    "zoom":7
  };
  navMap.restoreState(state);
});