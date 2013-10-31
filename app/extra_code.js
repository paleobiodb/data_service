  .service('intervalData', ['$http', function($http) {

    var intervalData = { tree: { oid: 0, color: "#000000", 
                                nam: "Geologic Time", "lvl":0, children: []}, 
                        populated: undefined, data: undefined };
    
    $http.get('/data1.1/intervals/list.json?order=older&max_ma=4000').then(function(response) {
          intervalData.data = response.data;
          for(var i=0; i < intervalData.data.records.length; i++) {
            var r = intervalData.data.records[i];
            r.children = [];
            r.pid = r.pid || 0;
            r.total = r.eag - r.lag;
            intervalData.tree[r.oid] = r;
            intervalData.tree[r.pid].children.push(r);
          }
          intervalData.populated = true;
        });

    return intervalData;
  }]);



  $scope.$watch(intervalData.populated, function() {
      $scope.data = intervalData.data;
      $scope.interval_hash = intervalData.hash
    });