/*
 * Copyright © 2015-2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceStreamMetadataController', function($scope, $stateParams, myHelpers, myStreamApi, $state, EventPipe, myAlertOnValium) {

    $scope.avro = {};

    $scope.formatOptions = ['avro', 'clf', 'csv', 'grok', 'syslog', 'text', 'tsv', 'stream'];

    var requestParams = {
      namespace: $stateParams.nsadmin,
      streamId: $stateParams.streamId,
      scope: $scope
    };

    $scope.reload = function () {

      myStreamApi.get(requestParams)
        .$promise
        .then(function (res) {
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
      $scope.error = null;

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

      var exceptions = ['clf', 'syslog', 'avro'];
      // do not include properties on the request when schema field is empty
      if (properties.length !== 0 && exceptions.indexOf($scope.format) === -1 ) {
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
      if (Object.keys(settings).length !== 0 && exceptions.indexOf($scope.format) === -1 ) {
        obj.settings = settings;
      }

      var params = {
        ttl: $scope.ttl,
        format: obj,
        'notification.threshold.mb': $scope.threshold
      };

      myStreamApi.setProperties(requestParams, params)
        .$promise
        .then(function() {
          $scope.reload();

          myAlertOnValium.show({
            type: 'success',
            title: 'Success',
            content: 'Stream properties have been successfully saved'
          });

        }, function (err) {
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

    $scope.deleteStream = function() {
      EventPipe.emit('showLoadingIcon');
      myStreamApi.delete(requestParams, {}, function success() {
        EventPipe.emit('hideLoadingIcon.immediate');

        $state.go('admin.namespace.detail.data', {}, {reload: true})
          .then(function () {
            myAlertOnValium.show({
              type: 'success',
              content: 'Successfully deleted stream'
            });
          });

      }, function error() {
        EventPipe.emit('hideLoadingIcon.immediate');
      });

    };

    $scope.enter = function (event, last, source) {
      if (last && event.keyCode === 13) {
        if (source === 'settings') {
          $scope.addSetting();
        } else if (source === 'preference') {
          $scope.addProperties();
        }
      } else {
        return;
      }
    };

  });
