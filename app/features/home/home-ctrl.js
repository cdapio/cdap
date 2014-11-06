/**
 * HomeCtrl
 */

angular.module(PKG.name+'.feature.home').controller('HomeCtrl',
function ($scope, $filter, $modal, $alert, myAuth, myApi, myFileReader) {

  var filterFilter = $filter('filter');


  if(myAuth.isAuthenticated()) {
    getData();
  }

  $scope.doRefresh = getData;


  $scope.doImport = function () {
    myFileReader.get()
      .then(function (reader) {
        return myApi.Import.save(angular.fromJson(reader.result));
      })
      .then(function (result) {
        $alert({
          title: 'Import complete!',
          type: 'success'
        });
        getData();
      })
      ['catch'](function (err) {
        $alert({
          title: 'import error!',
          content: err,
          type: 'danger'
        });
      });
  };



  $scope.doExport = function () {
    var modalScope = $scope.$new();

    modalScope.filename = 'export.json';

    $modal({
      scope: modalScope,
      title: 'JSON export',
      contentTemplate: '/assets/features/home/export.html',
      placement: 'center',
      show: true
    });

    myApi.Export.query(function (result) {
      var b = new Blob([ angular.toJson(result) ], { type : 'application/json' });
      modalScope.bloburl = window.URL.createObjectURL( b );
      modalScope.filesize = Math.ceil(b.size/1024) + 'KB';
    });
  };


  /* ----------------------------------------------------------------------- */

  function getData () {
    myApi.Cluster.query(function (list) {

      var active = filterFilter(list, {status:'active'});

      $scope.liveClusters = active.length;
      $scope.pendingClusters = filterFilter(list, {status:'pending'}).length;

      $scope.liveNodes = countNodes(active);
      $scope.totalNodes = countNodes(list);

      $scope.timestamp = new Date();
    });
  }


  function countNodes (list) {
    return list.reduce(function (memo, cluster) {
      return memo + cluster.numNodes;
    }, 0);
  }


});



