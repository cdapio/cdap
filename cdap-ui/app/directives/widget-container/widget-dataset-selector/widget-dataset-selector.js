angular.module(PKG.name + '.commons')
  .directive('myDatasetSelector', function () {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
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
          namespace: $state.params.namespace || $state.params.nsadmin
        };

        var dataMap = [];

        resource.list(params)
          .$promise
          .then(function (res) {
            $scope.list = res;

            dataMap = res.map(function (d) { return d.name; });
          });

        $scope.$watch('model', function () {
          if (!$scope.model || dataMap.indexOf($scope.model) === -1 ) { return; }

          if ($scope.datasetType === 'stream') {
            params.streamId = $scope.model;
          } else if ($scope.datasetType === 'dataset') {
            params.datasetId = $scope.model;
          }

          resource.get(params)
            .$promise
            .then(function (res) {
              var schema;

              if ($scope.datasetType === 'stream') {
                schema = JSON.stringify(res.format.schema);
              } else if ($scope.datasetType === 'dataset') {
                schema = res.spec.properties.schema;
              }
              EventPipe.emit('dataset.selected', schema);
            });
        });


        $scope.$on('$destroy', function () {
          EventPipe.cancelEvent('dataset.selected');
        });

      }
    };
  });
