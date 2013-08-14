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

pbdb_phylo_service.factory('taxonomy', ['$http', function($http) {
    function listTaxaByName(name, success_fn, error_fn)
    {
	$http.get('/data1.1/taxa/list.json?name=' + name).success(success_fn).error(error_fn);
    }
    
    function getTaxon(id, options, success_fn, error_fn)
    {
	var extra = '';
	if ( options.show )
	{
	    extra = extra + '&show=' + options.show;
	}
	$http.get('/data1.1/taxa/single.json?id=' + id + extra).success(success_fn).error(error_fn);
    }
    
    function taxonTitle(taxon)
    {
	if ( typeof taxon == "object" ) {
	    return taxon.nam + ' (' + taxon.rnk + ')';
	} else {
	    return '';
	}
    }
    
    return { listTaxaByName: listTaxaByName, 
	     getTaxon: getTaxon,
	     taxonTitle: taxonTitle
	   };
}]);


pbdb_phylo_service.factory('taxon', ['$resource', function($resource) {
    return $resource('/data1.1/taxa/single.json', {}, {});
}]);
