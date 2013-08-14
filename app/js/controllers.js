'use strict';

/* Controllers */

function Browser($scope, $http) {

    $scope.name_entry = '';


};




var interval_data = { oid: 0, color: "#000000", nam: "Geological Time", children: [] };
var interval_hash = { 0: interval_data };

//var intervalModule = angular.module('myApp.controllers', []);

//intervalModule.controller('IntervalData', [function($scope, $http) {

function IntervalData($scope, $http) {
	$http.get('/data1.1/intervals/list.json?order=older&max_ma=4000').success(function(data) {
	    $scope.raw_intervals = data;
	    
	    for(var i=0; i < data.records.length; i++) {
		var r = data.records[i];
		r.children = [];
		r.pid = r.pid || 0;
		interval_hash[r.oid] = r;
		interval_hash[r.pid].children.push(r);
	    }
	});
    };

//IntervalData.$inject = ['$scope', '$http'];


