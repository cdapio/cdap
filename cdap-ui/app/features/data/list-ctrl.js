angular.module(PKG.name + '.feature.data')
  .controller('CdapDataListController', function($state, $scope, MyOrderings, myStreamApi, myDatasetApi) {
    $scope.MyOrderings = MyOrderings;
    $scope.dataList = [];

    var params = {
      namespace: $state.params.namespace,
      scope: $scope
    };

    myStreamApi.list(params)
      .$promise
      .then(function (res) {
        $scope.dataList = res
          .map(function(dataset) {
            dataset.dataType = 'Stream';
            return dataset;
          })
          .concat($scope.dataList);
      });

    myDatasetApi.list(params)
      .$promise
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
          streamId: data.name
        });
      }
      MyOrderings.dataClicked(data.name);
    };

    $scope.goToList = function(data) {
      if (data.dataType === 'Dataset') {
        $state.go('datasets.list');
      } else if (data.dataType === 'Stream') {
        $state.go('streams.list');
      }
    };

  });
