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

      // Cleanup Properties from empty fields
      var properties = [];
      angular.forEach($scope.properties, function(p) {
        if (p.name) {
          properties.push({
            name: p.name,
            type: p.type
          });
        }
      });

      var obj = {
        name: $scope.format
      };

      // do not include properties on the request when schema field is empty
      if (properties.length !== 0) {
        obj.schema = {
          type: 'record',
          name: $stateParams.streamid + 'Body',
          fields: properties
        };
      }

      var settings = {};
      // cleanup settings
      angular.forEach($scope.settings, function(v) {
        if (v.key) {
          settings[v.key] = v.value;
        }
      });
      // do not include settings on request when there is no setting defined
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
      var index = $scope.properties.indexOf(property);
      $scope.properties.splice(index, 1);
    };

    $scope.addSetting = function() {
      $scope.settings.push({
        key: '',
        value: ''
      });
    };

    $scope.removeSetting = function(setting) {
      var index = $scope.settings.indexOf(setting);
      $scope.settings.splice(index, 1);
    };

    $scope.closeModal = function() {
      $modalInstance.close();

    };

  });
