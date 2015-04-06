angular.module(PKG.name + '.feature.admin')
  .controller('StreamPropertiesController', function($scope, MyDataSource, $modalInstance, $filter, $stateParams, myHelpers) {

    var dataSrc = new MyDataSource($scope);

    $scope.activePanel = 0;
    var filterFilter = $filter('filter');

    var basePath = '/namespaces/' + $stateParams.nsadmin + '/streams/' + $stateParams.streamid;
    $scope.formatOptions = ['avro', 'clf', 'csv', 'grok', 'syslog', 'text', 'tsv'];

    $scope.reload = function () {
      dataSrc
        .request({
          _cdapPath: basePath
        })
        .then(function(res) {
          $scope.ttl = myHelpers.objectQuery(res, 'ttl');
          $scope.format = myHelpers.objectQuery(res, 'format', 'name');
          $scope.threshold = myHelpers.objectQuery(res, 'notification.threshold.mb');
          $scope.properties = myHelpers.objectQuery(res, 'format', 'schema', 'fields');

          var settings = myHelpers.objectQuery(res, 'format', 'settings');
          $scope.settings = [];
          angular.forEach(settings, function(v, k) {
            $scope.settings.push({
              key: k,
              value: v
            });
          });
        });
    };

    $scope.reload();

    $scope.save = function() {

      var fields = JSON.parse(angular.toJson($scope.properties));

      var obj = {
        name: $scope.format
      };

      // do not include schema on the request when schema field is empty
      if (fields.length !== 0) {
        obj.schema = {
          fields: fields
        };
      }

      var settings = {};

      angular.forEach($scope.settings, function(v) {
        settings[v.key] = v.value;
      });

      if (Object.keys(settings).length !== 0) {
        obj.settings = settings;
      }

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
    };

    $scope.removeProperty = function(property) {
      var match = filterFilter($scope.properties, property);
      if (match.length) {
        $scope.properties.splice($scope.properties.indexOf(match[0]), 1);
      }
    };

    $scope.addSetting = function() {
      $scope.settings.push({
        key: '',
        value: ''
      });
    };

    $scope.removeSetting = function(setting) {
      var match = filterFilter($scope.settings, setting);
      if (match.length) {
        $scope.settings.splice($scope.settings.indexOf(match[0]), 1);
      }
    };

    $scope.closeModal = function() {
      $modalInstance.close();

    };

  });
