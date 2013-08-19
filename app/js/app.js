'use strict';


// Declare app level module which depends on filters, and services
//angular.module('myApp', ['myApp.filters', 'myApp.services', 'myApp.directives', 'myApp.controllers']).
var pbdb_app = angular.module('PBDBDemo', ['PBDBDemo.filters', 'PBDBDemo.services', 'PBDBDemo.directives', 'phyloService']);
var browser_scope;
var focal_taxon;

pbdb_app.config(['$routeProvider', function($routeProvider) {
    $routeProvider.when('/phylo=:phylo_selector', { templateUrl: 'partials/browser.html', controller: 'Browser' });
    $routeProvider.when('/view=:view_selector/phylo=:phylo_selector', { templateUrl: 'partials/browser.html', controller: 'Browser' });
    $routeProvider.when('/view=:view_selector', {templateUrl: 'partials/browser.html', controller: 'Browser'});
    $routeProvider.otherwise({redirectTo: '/view=mtp'});
  }]);

pbdb_app.controller('Browser', ['$scope', '$routeParams', '$location', 'phyloData', 
				function($scope, $routeParams, $location, phyloData) {
    
    browser_scope = $scope;	// for debugging
    $scope.name_entry = '';
    $scope.subTaxonOrder = 'size.desc';
    
    $scope.taxonTitle = function(t) {
	return phyloData.taxonTitle(t);
    };
    
    $scope.taxonRoute2 = function(t) {
	var rp = $routeParams;
	var route = '/view=' + rp.view_selector;
	if ( typeof t == "object" && t.gid )
	{
	    route += '/phylo=' + t.gid;
	}
	return route;
    };
    
    $scope.subTaxonOrderGetter = function(taxon) {
	if ( $scope.subTaxonOrder == 'size.desc' ) {
	    return 1000000 - taxon.siz;
	} else {
	    return taxon.nam;
	}
    };
    
    $scope.nameEntered = function() {
	if ( this.name_entry.length > 0 ) {
	    phyloData.listTaxaByName(this.name_entry, $scope.foundEnteredName, $scope.errorEnteredName);
	}
    };
    
    $scope.foundEnteredName = function(data) {
	if ( data.records.length > 0 ) {
	    $scope.name_entry = '';
	    $scope.jumpTaxon(data.records[0]);
	} else {
	    $scope.errorEnteredName({ error: "nothing found" });
	}
    };
    
    $scope.jumpTaxon = function(taxon) {
	$scope.focal_taxon = taxon;
	focal_taxon = taxon;
	phyloData.getTaxon(taxon.oid, { show: 'attr,nav,applong' }, $scope.finishJumpTaxon, $scope.errorEnteredName);
    };
    
    $scope.finishJumpTaxon = function(data) {
	if ( data.records.length > 0 ) {
	    $scope.focal_taxon = data.records[0];
	    focal_taxon = data.records[0];
	    $scope.focal_parents = computeParentList(data.records[0]);
	    $scope.focal_subsect = computeChildList(data.records[0]);
	    $location.path($scope.taxonRoute2($scope.focal_taxon));
	} else {
	    $scope.errorEnteredName({ error: "nothing found" });
	}
    };
    
    function computeParentList(taxon) {
	var parent_list = [];
	var last_oid = 0;
	
	if ( taxon.kgt && taxon.kgn && taxon.kgn != taxon.gid )
	{
	    taxon.kgt.rnk = 'kingdom*';
	    parent_list.push(taxon.kgt);
	    last_oid = taxon.kgn;
	}
	
	if ( taxon.phl && taxon.phn && taxon.phn != taxon.gid )
	{
	    taxon.pht.rnk = 'phylum*';
	    parent_list.push(taxon.pht);
	    last_oid = taxon.phn;
	}
	
	if ( taxon.cll && taxon.cln && taxon.cln != taxon.gid )
	{
	    taxon.clt.rnk = 'class*';
	    parent_list.push(taxon.clt);
	    last_oid = taxon.cln;
	}
	
	if ( taxon.odl && taxon.odn && taxon.odn != taxon.gid )
	{
	    taxon.odt.rnk = 'order*';
	    parent_list.push(taxon.odt);
	    last_oid = taxon.odn;
	}
	
	if ( taxon.fml && taxon.fmn && taxon.fmn != taxon.gid )
	{
	    taxon.fmt.rnk = 'family*';
	    parent_list.push(taxon.fmt);
	    last_oid = taxon.fmn;
	}
	
	if ( taxon.prt && taxon.par != last_oid )
	{
	    parent_list.push(taxon.prt);
	}
	
	return parent_list;
    }
   
    function computeChildList(taxon) {
	var section_list = [];
	
	if ( taxon.chl && taxon.rnk > 5 && ( taxon.chl.length == 0 || !taxon.gns || taxon.chl.length != taxon.gnc ) )
	{
	    section_list.push({ section: "immediate subtaxa", size: taxon.chl.length, 
				offset: 0, order: 'size.desc', taxa: taxon.chl });
	}
	
	if ( taxon.phs )
	{
	    section_list.push({ section: "phyla", size: taxon.phc, rank: 20, 
				offset: 0, max: 10, order: 'size.desc', taxa: taxon.phs });
	}
	
	if (taxon.cls )
	{
	    section_list.push({ section: "classes", size: taxon.clc, rank: 17, 
				offset: 0, max: 10, order: 'size.desc', taxa: taxon.cls });
	}
	
	if (taxon.ods )
	{
	    section_list.push({ section: "orders", size: taxon.odc, rank: 13, 
				offset: 0, max: 10, order: 'size.desc', taxa: taxon.ods });
	}
	
	if (taxon.fms )
	{
	    section_list.push({ section: "families", size: taxon.fmc, rank: 9, 
				offset: 0, max: 10, order: 'size.desc', taxa: taxon.fms });
	}
	
	if ( taxon.gns )
	{
	    section_list.push({ section: "genera", size: taxon.gnc, rank: 5, 
				offset: 0, max: 10, order: 'size.desc', taxa: taxon.gns });
	}
	
	if ( taxon.sgs && taxon.sgs.length > 0 )
	{
	    section_list.push({ section: "subgenera", size: taxon.gnc, rank: 4, 
				offset: 0, max: 10, order: 'size.desc', taxa: taxon.sgs });
	}
	
	if ( taxon.sps && taxon.sps.length > 0 )
	{
	    section_list.push({ section: "species", size: taxon.sps.length, rank: 3, 
				offset: 0, max: 10, order: 'size.desc', taxa: taxon.sps });
	}
	
	if ( taxon.sss && taxon.sss.length > 0 )
	{
	    section_list.push({ section: "subspecies", size: taxon.sss.length, rank: 2, 
				offset: 0, max: 10, order: 'size.desc', taxa: taxon.sss });
	}
	
	return section_list;
    };
    
    $scope.showAllSubTaxa = function(s) {
	
	function finishSubTaxa (data) {
	    if ( data.records.length > 0 )
	    {
		s.taxa = data.records;
	    }	    
	};
	
	if ( typeof s == "object" )
	{
	    phyloData.getSubtaxa(focal_taxon.oid, s.rank, 0, 100, finishSubTaxa);
	}
    }
    
    $scope.showAllIsVisible = function(s) {
	return (s.size > s.taxa.length);
    }
    
    $scope.errorEnteredName = function(e) {
	$scope.error_object = e;
	alert('An error occurred: ' + e);
    };
    
    var phylo_selector = $routeParams.phylo_selector;
    
    if ( phylo_selector )
    {
    	phyloData.getTaxon(phylo_selector, { show: 'attr,nav,applong' }, $scope.finishJumpTaxon);
    }
    // else
    // {
    // 	phyloData.getTaxon(1, { show: 'attr,nav' }, $scope.finishJumpTaxon);
    // }
    
}]);

	    
	    // if ( taxon_id > 0 )
	    // {
	    // 	this.name_entry = '';
	    // 	this.focusOnTaxon(taxon_id);
	    // }
