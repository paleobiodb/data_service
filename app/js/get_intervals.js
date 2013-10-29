var midpoints = [],
      uniqueMidpoints = [];

if (r.eag < 600) {
  midpoints.push(parseInt((r.eag + r.lag) / 2))
}

$.each(midpoints, function(i, el){
  if($.inArray(el, uniqueMidpoints) === -1) uniqueMidpoints.push(el);
});
uniqueMidpoints = uniqueMidpoints.sort(d3.ascending);
console.log(JSON.stringify(uniqueMidpoints));