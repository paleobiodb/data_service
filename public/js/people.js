$("#community").on("click", function() {
  scrollTo("members");
});
var peopleGraph = {
  "init": function() {
    var margin = {top: 0, right: 0, bottom: 0, left: 0},
        width = parseInt(d3.select("html").style("width")) - margin.left - margin.right,
        height = 700 - margin.top - margin.bottom,
        n = 200,
        m = 10;
        nodes = [];

    var legend = d3.select("#peopleChartLegend").append("svg")
        .attr("height", 50)
        .attr("width", 250);

      legend.append("circle")
        .attr("r", 20)
        .attr("cx", 25)
        .attr("cy", 25)
        .attr("fill", "none")
        .attr("class", "leader");

      legend.append("text")
        .attr("x", 55)
        .attr("y", 40)
        .text("Holds leadership position");

      d3.json("js/people_sans_members.json", function(err, data) {
          data = data.children;
          for (var i=0; i<data.length;i++) {
            for(var j=0; j<data[i].children.length; j++) {
              data[i].children[j].type = data[i].name;
              data[i].children[j].radius = 54;
              data[i].children[j].color = peopleGraph.getFill(data[i].children[j].type);
              nodes.push(data[i].children[j]);
            }
          }

        var force = d3.layout.force()
            .nodes(nodes)
            .size([width, height])
            .gravity(.1)
            .charge(0)
            .on("tick", peopleGraph.tick)
            .start();

        var svg = d3.select("#peopleChart").append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .attr("id", "peopleChartSvg")
          .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

          circle = svg.selectAll("circle")
            .data(nodes)
          .enter().append("g")
            .attr("class", "peopleCircle")
            .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; })
            .call(force.drag);

          circle.append("circle")
            .attr("r", function(d) { return d.radius; })
            .style("fill", function(d) { return d.color; })
            .attr("class", function(d) {
              if (d.role) {
                return "leader";
              }
            })

          circle.append("foreignObject")
            .attr("text-anchor", "middle")
            .attr("width", "100px")
            .attr("height", "100px")
            .attr("x", -50)
            .attr("y", -25)
            .style("color", "#000")
            .append("xhtml:div")
            .html(function(d) { 
              var name = d.name.split(/[ ,]+/);
              return "<p>" + name[0] + "<br>" + name[1] + "</p>"})
            .style("font-size", "15px");

          circle.append("title")
            .text(function(d) {return d.name +  " - " + d.institute;});

          resize();
        }); // end JSON callback
  },
  "getFill": function(type) {
    switch(type) {
      case "Executive Committee":
        return "#985144";
      case "Tech Team":
        return "#489FB2";
      case "Advisory Board":
        return "#8DA0CB";
      case "Members":
        return "#66C2A5";
    }
  },
  "tick": function(e) {
    circle
      .each(peopleGraph.cluster(10 * e.alpha * e.alpha))
      .each(peopleGraph.collide(.5))
      .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; })
  },
  "cluster": function(alpha) {
    var max = {};

    // Find the largest node for each cluster.
    nodes.forEach(function(d) {
      if (!(d.color in max) || (d.radius > max[d.color].radius)) {
        max[d.color] = d;
      }
    });

    return function(d) {
      var node = max[d.color],
          l,
          r,
          x,
          y,
          i = -1;

      if (node == d) return;

      x = d.x - node.x;
      y = d.y - node.y;
      l = Math.sqrt(x * x + y * y);
      r = d.radius + node.radius;
      if (l != r) {
        l = (l - r) / l * alpha;
        d.x -= x *= l;
        d.y -= y *= l;
        node.x += x;
        node.y += y;
      }
    };
  },
  "collide": function(alpha) {
    var radius = d3.scale.sqrt().range([0, 12]),
        padding = 6,
        quadtree = d3.geom.quadtree(nodes);

    return function(d) {
      var r = d.radius + radius.domain()[1] + padding,
          nx1 = d.x - r,
          nx2 = d.x + r,
          ny1 = d.y - r,
          ny2 = d.y + r;
      quadtree.visit(function(quad, x1, y1, x2, y2) {
        if (quad.point && (quad.point !== d)) {
          var x = d.x - quad.point.x,
              y = d.y - quad.point.y,
              l = Math.sqrt(x * x + y * y),
              r = d.radius + quad.point.radius + (d.color !== quad.point.color) * padding;
          if (l < r) {
            l = (l - r) / l * alpha;
            d.x -= x *= l;
            d.y -= y *= l;
            quad.point.x += x;
            quad.point.y += y;
          }
        }
        return x1 > nx2
            || x2 < nx1
            || y1 > ny2
            || y2 < ny1;
      });
    };
  }
} // End peopleGraph

var peopleMap = {
  "init": function() {
    var projection = d3.geo.mollweide()
       .precision(0.3);

    var path = d3.geo.path()
       .projection(projection);

    var scale = d3.scale.linear()
      .domain([0,10])
      .range(["#E5F5F9", "#2CA25F"]);

    var svg = d3.select("#memberMap").append("svg")
      .attr("width", parseInt(d3.select("#memberMap").style("width")))
      .attr("height", 450)
      .append("g")
      .attr("id", "memberMapG");

    d3.json("js/countries.json", function(err, countries) {
      var boundaries = topojson.feature(countries, countries.objects.countries);
      d3.tsv("people.csv", function(error, peeps) {
        var people = peeps;

        var na = people.filter(function(d) {return d.continent == "North America"});
        var sa = people.filter(function(d) {return d.continent == "South America"});
        var europe = people.filter(function(d) {return d.continent == "Europe"});
        var oceania = people.filter(function(d) {return d.continent == "Oceania"});
        var asia = people.filter(function(d) {return d.continent == "Asia"});

        for (var i=0; i<na.length; i++) {
          var tr = d3.select("#naTbody")
            .append("tr")
            .attr("id", function(d) {
              return "t" + na[i].id;
            });

          tr.append("td")
            .text(na[i].first + " " + na[i].last);
          tr.append("td")
            .text(na[i].institute);

        }
        for (var i=0; i<sa.length; i++) {
          var tr = d3.select("#saTbody")
            .append("tr")
            .attr("id", function(d) {
              return "t" + sa[i].id;
            });

          tr.append("td")
            .text(sa[i].first + " " + sa[i].last);
          tr.append("td")
            .text(sa[i].institute);

        }
        for (var i=0; i<europe.length; i++) {
          var tr = d3.select("#europeTbody")
            .append("tr")
            .attr("id", function(d) {
              return "t" + europe[i].id;
            });

          tr.append("td")
            .text(europe[i].first + " " + europe[i].last);
          tr.append("td")
            .text(europe[i].institute);

        }
        for (var i=0; i<oceania.length; i++) {
          var tr = d3.select("#oceaniaTbody")
            .append("tr")
            .attr("id", function(d) {
              return "t" + oceania[i].id;
            });

          tr.append("td")
            .text(oceania[i].first + " " + oceania[i].last);
          tr.append("td")
            .text(oceania[i].institute);

        }
        for (var i=0; i<asia.length; i++) {
          var tr = d3.select("#asiaTbody")
            .append("tr")
            .attr("id", function(d) {
              return "t" + asia[i].id;
            });

          tr.append("td")
            .text(asia[i].first + " " + asia[i].last);
          tr.append("td")
            .text(asia[i].institute);

        }
        svg.selectAll(".countries")
        .data(boundaries.features)
        .enter().append("path")
        .attr("class", "land")
        .attr("id", function(d) {return d.properties.continent})
        .attr("fill", "#777")
        .attr("d", path)
        .on("click", function() {
          d3.selectAll("tbody").style("display", "none");
          var id =this.id;
          switch (id) {
            case "North America":
              d3.select("#naTbody").style("display", "table-row-group");
              break;
            case "South America":
              d3.select("#saTbody").style("display", "table-row-group");
              break;
            case "Europe":
              d3.select("#europeTbody").style("display", "table-row-group");
              break;
            case "Oceania":
              d3.select("#oceaniaTbody").style("display", "table-row-group");
              break;
            case "Asia":
              d3.select("#asiaTbody").style("display", "table-row-group");
              break;
          }
        })
        .on("mouseover", function() {
          d3.selectAll("tbody").style("display", "none");
          var id =this.id;
          switch (id) {
            case "North America":
              d3.select("#naTbody").style("display", "table-row-group");
              break;
            case "South America":
              d3.select("#saTbody").style("display", "table-row-group");
              break;
            case "Europe":
              d3.select("#europeTbody").style("display", "table-row-group");
              break;
            case "Oceania":
              d3.select("#oceaniaTbody").style("display", "table-row-group");
              break;
            case "Asia":
              d3.select("#asiaTbody").style("display", "table-row-group");
              break;
          }
        });

        svg.selectAll(".places")
          .data(people)
        .enter().append("circle")
          .attr("cx", function(d) {
            return projection([d.longitude, d.latitude])[0];
          })
          .attr("cy", function(d) {
            return projection([d.longitude, d.latitude])[1];
          })
          .attr("r", 5)
          .attr("class", "places")
          .attr("id", function(d) {
            return "p" + d.id;
          })
          .style("display", "none")
          .style("fill", "#2CA25F");

        d3.selectAll("tr").on("mouseover", function(d) {
          var id = this.id;
          id = id.substring(1);
          d3.select("#p" + id).style("display", "block");
        })
        .on("mouseout", function(d) {
          d3.selectAll(".places").style("display", "none");
        })
        .on("click", function(d) {
          d3.selectAll(".places").style("display", "none");
          var id = this.id;
          id = id.substring(1);
          d3.select("#p" + id).style("display", "block");
        });

      });
      d3.select("#memberMapG")
      .attr("transform", function(d) {
        return "scale(" + parseInt(d3.select("#memberMap").style("width"))/900 + ")translate(-125, 0)"; 
      });

    });
  } // end peopleMap.init();
} // end peopleMap()

function setup() {
  resize();

  //attach window resize listener to the window
  d3.select(window).on("resize", resize);

  // Build the people graph
  peopleGraph.init();

  // Build the people map
  peopleMap.init();

resize();

} // end setup()

function resize() {

  var windowHeight = window.innerHeight - 80,
    row1Height = windowHeight * 0.6 + "px",
    row2Height = windowHeight * 0.4 + 40 + "px";

  d3.select("#peopleChartSvg").select("g")
    .attr("transform", function(d) {
      var scale = parseInt(d3.select("#peopleChart").style("width"))/900;
      scale = (scale > 1.1) ? 1.1 : scale;
      return "scale(" + scale + ")";
    });

  d3.select("#peopleChartSvg")
    .attr("height", function(d) {
      return parseInt(d3.select("#peopleChart").style("width"))*0.7 + "px";
    })
    .attr("width", function(d) {
      return parseInt(d3.select("html").style("width")) - 20 + "px";
    });

  d3.select("#memberChartSvg")
    .attr("transform", function(d) {
      return "scale(" + parseInt(d3.select("#chart").style("width"))/961 + ")";
    });

  d3.select("#memberMap").select("svg")
    .attr("width", function() {
      return d3.select("#memberMap").style("width");
    })
    .attr("height", function() {
      return parseInt(d3.select("#memberMap").style("width")) * 0.5 + "px";
    });

  d3.select("#memberMapG")
    .attr("transform", function(d) {
      return "scale(" + parseInt(d3.select("#memberMap").style("width"))/900 + ")translate(-125, 0)"; 
    });
} // end resize()

setup();