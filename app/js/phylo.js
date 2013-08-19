'use strict';

/* Service for querying phylogenic data */

var pbdb_phylo_service = angular.module('phyloService', []);

// pbdb_phylo_service.factory('taxonomy', ['$http', function($http) {
//     var new_service = {};
//     new_service.listTaxaByName = function(name, success_fn, error_fn)
//     {
// 	$http.get('/data1.1/taxa/list.json?name=' + name); //.success(success_fn).error(error_fn);
//     };
    
//     return new_service;
// }]);

pbdb_phylo_service.factory('phyloData', ['$http', function($http) {
    
    var rankMap = { 25: "unranked", 23: "kingdom", 22: "subkingdom",
		    21: "superphylum", 20: "phylum", 19: "subphylum",
		    18: "superclass", 17: "class", 16: "subclass", 15: "infraclass",
		    14: "superorder", 13: "order", 12: "suborder", 11: "infraorder",
		    10: "superfamily", 9: "family", 8: "subfamily",
		    7: "tribe", 6: "subtribe", 5: "genus", 4: "subgenus",
		    3: "species", 2: "subspecies" };
    
    function listTaxaByName(name, success_fn, error_fn)
    {
	var url = '/data1.1/taxa/list.json?name=' + name;
	$http.get(url).success(success_fn).error(error_fn);
    }
    
    function getTaxon(id, options, success_fn, error_fn)
    {
	var extra = '';
	if ( options.show )
	{
	    extra = extra + '&show=' + options.show;
	}
	var url = '/data1.1/taxa/single.json?id=' + id + extra;
	$http.get(url).success(success_fn).error(error_fn);
    }
    
    function getSubtaxa(id, rank, offset, limit, success_fn, error_fn)
    {
	var lim_str = '';
	
	if ( typeof offset == "number" )
	{
	    lim_str += '&offset=' + offset;
	}
	
	if ( typeof limit == "number" )
	{
	    lim_str += '&limit=' + limit;
	}
	
	if ( rank > 0 )
	{
	    var url = '/data1.1/taxa/list.json?id=' + id + lim_str + '&show=size,appfirst&rel=all_children&rank=' + rank;
	    $http.get(url).success(success_fn).error(error_fn);
	}
    }
    
    function rankLabel(rank)
    {
	return rankMap[rank] || rank;
    }
    
    function taxonTitle(taxon)
    {
	if ( typeof taxon == "object" ) {
	    return taxon.nam + ' (' + rankLabel(taxon.rnk) + ')';
	} else {
	    return '';
	}
    }
    
    return { listTaxaByName: listTaxaByName, 
	     getTaxon: getTaxon,
	     getSubtaxa: getSubtaxa,
	     rankLabel: rankLabel,
	     taxonTitle: taxonTitle
	   };
}]);


pbdb_phylo_service.factory('taxon', ['$resource', function($resource) {
    return $resource('/data1.1/taxa/single.json', {}, {});
}]);
