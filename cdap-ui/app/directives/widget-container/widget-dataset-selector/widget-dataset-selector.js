angular.module(PKG.name + '.commons')
  .directive('myDatasetSelector', function () {
    return {
      restrict: 'E',
      scope: {
        datasetType: '@'
      },
      templateUrl: 'widget-container/widget-dataset-selector/widget-dataset-selector.html',
      controller: function ($scope, myDatasetApi, myStreamApi, $state, EventPipe) {
        var resource;
        $scope.list = [];

        if ($scope.datasetType === 'stream') {
          resource = myStreamApi;
        } else if ($scope.datasetType === 'dataset') {
          resource = myDatasetApi;
        }

        var params = {
          namespace: $state.params.namespace
        };

        resource.list(params)
          .$promise
          .then(function (res) {
            $scope.list = res;
          });

        $scope.$watch('selected', function () {
          if (!$scope.selected) { return; }
console.log('selected', $scope.selected);
          if ($scope.datasetType === 'stream') {
            params.streamId = $scope.selected;
          } else if ($scope.datasetType === 'dataset') {
            params.datasetI = $scope.selected;
          }

          resource.get(params)
            .$promise
            .then(function (res) {
              var schema = JSON.stringify(res.format.schema);
              EventPipe.emit('dataset.selected', schema);
            });


        });


        $scope.$on('$destroy', function () {
          EventPipe.cancelEvent('dataset.selected');
        });

      }
    };
  });
