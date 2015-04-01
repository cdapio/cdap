angular.module(PKG.name + '.feature.admin')
  .controller('StreamPropertiesController', function($scope, MyDataSource, $modalInstance, $filter, $stateParams, myHelpers) {

    var dataSrc = new MyDataSource($scope);

    $scope.activePanel = 0;
    var filterFilter = $filter('filter');

    var basePath = '/namespaces/' + $stateParams.nsadmin + '/streams/' + $stateParams.streamid;

    $scope.reload = function () {
      dataSrc
        .request({
          _cdapPath: basePath
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
          _cdapPath: basePath + '/properties',
          method: 'PUT',
          body: params
        })
        .then(function(res) {
          $modalInstance.close(res);
        }, function(err) {
          $scope.error = err;
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

    $scope.closeModal = function() {
      $modalInstance.close();

    };

  });
