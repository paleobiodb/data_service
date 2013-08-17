'use strict';


// Declare app level module which depends on filters, and services
//angular.module('myApp', ['myApp.filters', 'myApp.services', 'myApp.directives', 'myApp.controllers']).
var pbdb_app = angular.module('PBDBDemo', ['PBDBDemo.filters', 'PBDBDemo.services', 'PBDBDemo.directives', 'phyloService']);
var browser_scope;

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
    
    $scope.nameEntered = function() {
	if ( this.name_entry.length > 0 ) {
	    phyloData.listTaxaByName(this.name_entry, $scope.foundEnteredName, $scope.errorEnteredName);
	}
    };
    
    $scope.foundEnteredName = function(data) {
	if ( data.records.length > 0 ) {
	    $scope.name_entry = '';
	    $scope.focal_taxon = data.records[0];
	    phyloData.getTaxon(data.records[0].oid, { show: 'attr,nav' }, $scope.finishJumpTaxon, $scope.errorEnteredName);
	} else {
	    $scope.errorEnteredName({ error: "nothing found" });
	}
    };
    
    $scope.jumpTaxon = function(taxon) {
	$scope.focal_taxon = taxon;
	phyloData.getTaxon(taxon.oid, { show: 'attr,nav' }, $scope.finishJumpTaxon, $scope.errorEnteredName);
    };
    
    $scope.finishJumpTaxon = function(data) {
	if ( data.records.length > 0 ) {
	    $scope.focal_taxon = data.records[0];
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
	
	if ( taxon.kgl && taxon.kgn && taxon.kgn != taxon.gid )
	{
	    parent_list.push({ oid: taxon.kgn, rnk: 'kingdom*', nam: taxon.kgl });
	    last_oid = taxon.kgn;
	}
	
	if ( taxon.phl && taxon.phn && taxon.phn != taxon.gid )
	{
	    parent_list.push({ oid: taxon.phn, rnk: 'phylum*', nam: taxon.phl });
	    last_oid = taxon.phn;
	}
	
	if ( taxon.cll && taxon.cln && taxon.cln != taxon.gid )
	{
	    parent_list.push({ oid: taxon.cln, rnk: 'class*', nam: taxon.cll });
	    last_oid = taxon.cln;
	}
	
	if ( taxon.odl && taxon.odn && taxon.odn != taxon.gid )
	{
	    parent_list.push({ oid: taxon.odn, rnk: 'order*', nam: taxon.odl });
	    last_oid = taxon.odn;
	}
	
	if ( taxon.fml && taxon.fmn && taxon.fmn != taxon.gid )
	{
	    parent_list.push({ oid: taxon.fmn, rnk: 'family*', nam: taxon.fml });
	    last_oid = taxon.fmn;
	}
	
	if ( taxon.prl && taxon.par != last_oid )
	{
	    parent_list.push({ oid: taxon.par, nam: taxon.prl, rnk: taxon.prr, imm: 1 });
	}
	
	return parent_list;
    }
   
    function computeChildList(taxon) {
	var section_list = [];
	
	if ( taxon.chl )
	{
	    section_list.push({ section: "immediate subtaxa", size: taxon.chl.length, taxa: taxon.chl });
	}
	
	if ( taxon.phs )
	{
	    section_list.push({ section: "phyla", size: taxon.phc, taxa: taxon.phs });
	}
	
	if (taxon.cls )
	{
	    section_list.push({ section: "classes", size: taxon.clc, taxa: taxon.cls });
	}
	
	if (taxon.ods )
	{
	    section_list.push({ section: "orders", size: taxon.odc, taxa: taxon.ods });
	}
	
	if (taxon.fms )
	{
	    section_list.push({ section: "families", size: taxon.fmc, taxa: taxon.fms });
	}
	
	if ( taxon.gns )
	{
	    section_list.push({ section: "genera", size: taxon.gnc, taxa: taxon.gns });
	}
	
	return section_list;
    };				    
				    
    $scope.errorEnteredName = function(e) {
	$scope.error_object = e;
	alert('An error occurred: ' + e);
    };
    
    if ( $routeParams.phylo_selector )
    {
	phyloData.getTaxon($routeParams.phylo_selector, { show: 'attr,nav' }, $scope.finishJumpTaxon, $scope.errorEnteredName);
    }
    

    
}]);

	    
	    // if ( taxon_id > 0 )
	    // {
	    // 	this.name_entry = '';
	    // 	this.focusOnTaxon(taxon_id);
	    // }
