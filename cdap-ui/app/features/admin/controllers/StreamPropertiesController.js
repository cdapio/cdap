angular.module(PKG.name + '.feature.admin')
  .controller('StreamPropertiesController', function($scope, MyDataSource, $stateParams, myHelpers, $alert) {

    var dataSrc = new MyDataSource($scope);
    $scope.avro = {};

    var basePath = '/namespaces/' + $stateParams.nsadmin + '/streams/' + $stateParams.streamId;
    $scope.formatOptions = ['avro', 'clf', 'csv', 'grok', 'syslog', 'text', 'tsv', 'stream'];

    $scope.reload = function () {
      dataSrc
        .request({
          _cdapPath: basePath
        })
        .then(function(res) {
          $scope.ttl = myHelpers.objectQuery(res, 'ttl');
          $scope.format = myHelpers.objectQuery(res, 'format', 'name');
          $scope.threshold = myHelpers.objectQuery(res, 'notification.threshold.mb');
          $scope.avro.schema = myHelpers.objectQuery(res, 'format', 'schema');
          var properties = myHelpers.objectQuery(res, 'format', 'schema', 'fields');
          $scope.properties = [];
          angular.forEach(properties, function(p) {
            if (angular.isArray(p.type)) {
              $scope.properties.push({
                name: p.name,
                type: p.type[0],
                nullable: true
              });
            } else if (angular.isObject(p.type)) {
              $scope.properties.push({
                name: p.name,
                type: p.type.items,
                nullable: false
              });
            } else {
              $scope.properties.push({
                name: p.name,
                type: p.type,
                nullable: false
              });

            }
          });

          // formatting settings
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
            type: p.nullable ? [p.type, 'null'] : p.type
          });
        }
      });

      var obj = {
        name: $scope.format === 'stream' ? 'text' : $scope.format
      };

      // do not include properties on the request when schema field is empty
      if (properties.length !== 0 && $scope.format !== 'clf' && $scope.format !== 'syslog' && $scope.format !== 'avro') {
        obj.schema = {
          type: 'record',
          name: $stateParams.streamid + 'Body',
          fields: properties
        };
      } else if ($scope.format === 'avro') {
        obj.schema = $scope.avro.schema;
      }

      var settings = {};
      // cleanup settings
      angular.forEach($scope.settings, function(v) {
        if (v.key) {
          settings[v.key] = v.value;
        }
      });
      // do not include settings on request when there is no setting defined
      if (Object.keys(settings).length !== 0 && $scope.format !== 'clf' && $scope.format !== 'syslog' && $scope.format !== 'avro') {
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
          $scope.reload();

          $alert({
            type: 'success',
            title: 'Success',
            content: 'Stream properties have been successfully saved!'
          });

        }, function(err) {
          $scope.error = err;
        });
    };

    $scope.$watch('format', function() {
      if ($scope.format === 'stream') {
        $scope.properties = [{
          name: 'body',
          type: 'string'
        }];

        $scope.settings = [];
        $scope.disableButtons = false;
        return;
      } else if ($scope.format !== 'grok') {
        $scope.disableButtons = false;
        return;
      }

      $scope.disableButtons = true;

      $scope.settings = [{
        key: 'pattern',
        value: $scope.settings[0] ? $scope.settings[0].value : ''
      }];
    });

    $scope.addProperties = function() {
      $scope.properties.push({
        name: '',
        type: 'string'
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

  });
