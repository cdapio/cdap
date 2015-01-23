angular.module(PKG.name + '.feature.data')
  .controller('CdapDataListController', function($state, $scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope);
    $scope.dataList = [];
    dataSrc.request({
      _cdapPathV2: '/streams'
    })
      .then(function(res) {
        res = res.map(function(dataset) {
          dataset.dataType = "Stream";
          return dataset;
        });
        $scope.dataList = res.concat($scope.dataList);

      });

    dataSrc.request({
      _cdapPathV2: '/datasets'
    })
      .then(function(res) {
        res = res.map(function(stream) {
          stream.dataType = "Dataset";
          return stream;
        });
        $scope.dataList = res.concat($scope.dataList);
      });

    $scope.goToDetail = function(data) {
      if (data.dataType === 'Dataset') {
        $state.go('datasets.detail.overview', {
          datasetId: data.name
        });
      } else if (data.dataType === 'Stream') {
        $state.go('streams.detail.overview', {
          streamId: data.id
        });
      }
    };

    $scope.goToList = function(data) {
      if (data.dataType === 'Dataset') {
        $state.go('datasets.list');
      } else if (data.dataType === 'Stream') {
        $state.go('streams.list');
      }
    };

  });
