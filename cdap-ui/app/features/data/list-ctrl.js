angular.module(PKG.name + '.feature.data')
  .controller('CdapDataListController', function($state, $scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope);
    $scope.dataList = [];
    dataSrc.request({
      _cdapNsPath: '/streams'
    })
      .then(function(res) {
        $scope.dataList = res
          .map(function(dataset) {
            dataset.dataType = 'Stream';
            return dataset;
          })
          .concat($scope.dataList);
      });

    dataSrc.request({
      _cdapNsPath: '/data/datasets'
    })
      .then(function(res) {
        $scope.dataList = res
          .map(function(stream) {
            stream.dataType = 'Dataset';
            return stream;
          })
          .concat($scope.dataList);
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
