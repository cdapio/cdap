angular.module(PKG.name + '.feature.streams')
  .controller('StreamDetailPropertiesController', function($scope, MyDataSource, $filter, $state, myHelpers, $alert) {

    var basePath = '/streams/' + $state.params.streamId;

    var filterFilter = $filter('filter');
    var dataSrc = new MyDataSource($scope);

    $scope.reload = function () {
      dataSrc
        .request({
          _cdapNsPath: basePath
        })
        .then(function(res) {
          $scope.ttl = myHelpers.objectQuery(res, 'ttl');
          $scope.format = myHelpers.objectQuery(res, 'format', 'name');
          $scope.type = myHelpers.objectQuery(res, 'format', 'schema', 'type');
          $scope.schemaName = myHelpers.objectQuery(res, 'format', 'schema', 'name');
          $scope.delimiter = myHelpers.objectQuery(res, 'format', 'settings', 'delimiter');
          $scope.threshold = myHelpers.objectQuery(res, 'notification.threshold.mb');
          $scope.properties = myHelpers.objectQuery(res, 'format', 'schema', 'fields');
        });
    };

    $scope.reload();

    $scope.save = function() {

      var fields = JSON.parse(angular.toJson($scope.properties));

      var obj = {
        name: $scope.format,
        schema: {
          type: $scope.type,
          name: $scope.schemaName,
          fields: fields
        }
      };

      var params = {
        ttl: $scope.ttl,
        format: obj,
        "notification.threshold.mb": $scope.threshold
      };

      dataSrc
        .request({
          _cdapNsPath: basePath + '/properties',
          method: 'PUT',
          body: params
        })
        .then(function(res) {
          $alert({
            content: 'Your changes have been successfully saved',
            type: 'success'
          });
          $scope.reload();
        });
    };

    $scope.addProperties = function() {
      $scope.properties.push({
        name: '',
        type: ''
      });
    }

    $scope.removeProperty = function(property) {
      var match = filterFilter($scope.properties, property);
      if (match.length) {
        $scope.properties.splice($scope.properties.indexOf(match[0]), 1);
      }
    };

  });
