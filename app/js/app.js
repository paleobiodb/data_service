'use strict';


// Declare app level module which depends on filters, and services
//angular.module('myApp', ['myApp.filters', 'myApp.services', 'myApp.directives', 'myApp.controllers']).
var pbdb_app = angular.module('PBDBDemo', ['PBDBDemo.filters', 'PBDBDemo.services', 'PBDBDemo.directives', 'phyloService']);
var browser_scope;

pbdb_app.config(['$routeProvider', function($routeProvider) {
    $routeProvider.when('/taxon_id=:taxon_id', { templateUrl: 'partials/browser.html', controller: 'Browser' });
    $routeProvider.when('/view=:view_selector/taxon_id=:taxon_id', { templateUrl: 'partials/browser.html', controller: 'Browser' });
    $routeProvider.when('/view=:view_selector', {templateUrl: 'partials/browser.html', controller: 'Browser'});
    $routeProvider.otherwise({redirectTo: '/view=mtp'});
  }]);

pbdb_app.controller('Browser', ['$scope', '$routeParams', '$location', 'taxonomy', 
				function($scope, $routeParams, $location, taxonomy) {
    
    browser_scope = $scope;	// for debugging
    $scope.name_entry = '';
    
    $scope.taxonTitle = function(t) {
	return taxonomy.taxonTitle(t);
    };
    
    $scope.taxonRoute2 = function(t) {
	var foo = $routeParams;
	var route = '/view=' + foo.view_selector;
	if ( typeof t == "object" && t.oid )
	{
	    route += '/taxon_id=' + t.oid;
	}
	return route;
    };
    
    $scope.nameEntered = function() {
	if ( this.name_entry.length > 0 ) {
	    taxonomy.listTaxaByName(this.name_entry, $scope.foundEnteredName, $scope.errorEnteredName);
	}
    };
    
    $scope.foundEnteredName = function(data) {
	if ( data.records.length > 0 ) {
	    $scope.name_entry = '';
	    $scope.focal_taxon = data.records[0];
	    taxonomy.getTaxon(data.records[0].oid, { show: 'nav' }, $scope.finishJumpTaxon, $scope.errorEnteredName);
	} else {
	    $scope.errorEnteredName({ error: "nothing found" });
	}
    };
    
    $scope.jumpTaxon = function(taxon) {
	$scope.focal_taxon = taxon;
	taxonomy.getTaxon(taxon.oid, { show: 'nav' }, $scope.finishJumpTaxon, $scope.errorEnteredName);
    };
    
    $scope.finishJumpTaxon = function(data) {
	if ( data.records.length > 0 ) {
	    $scope.focal_taxon = data.records[0];
	    $location.path($scope.taxonRoute2($scope.focal_taxon));
	} else {
	    $scope.errorEnteredName({ error: "nothing found" });
	}
    };
    
    $scope.errorEnteredName = function(e) {
	$scope.error_object = e;
	alert('An error occurred: ' + e);
    };
    
    if ( $routeParams.taxon_id )
    {
	taxonomy.getTaxon($routeParams.taxon_id, { show: 'nav' }, $scope.finishJumpTaxon, $scope.errorEnteredName);
    }
    

    
}]);

	    
	    // if ( taxon_id > 0 )
	    // {
	    // 	this.name_entry = '';
	    // 	this.focusOnTaxon(taxon_id);
	    // }
