/*
 * Copyright © 2015 Cask Data, Inc.
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
  .controller('NamespaceSettingsController', function ($scope, MyCDAPDataSource, $state, myAlertOnValium, $timeout, myNamespace, EventPipe) {

    var dataSrc = new MyCDAPDataSource($scope);
    $scope.loading = false;

    dataSrc.request({
      _cdapPath: '/namespaces/' + $state.params.nsadmin
    })
    .then(function (res) {
      $scope.description = res.description;
    });

    $scope.save = function() {

      dataSrc.request({
        _cdapPath: '/namespaces/' + $state.params.nsadmin + '/properties',
        method: 'PUT',
        body: {
          'description': $scope.description
        }
      })
      .then(function () {
        myAlertOnValium.show({
          type: 'success',
          content: 'Namespace successfully updated'
        });
      });

    };

    /**
     *  On namespace delete, we have to clean up the User Store as well.
     *    1. Plugin Templates
     *    2. Hydrator Drafts
     *    3. Operation Dashboard
     **/
    $scope.deleteNamespace = function() {
      $scope.loading = true;
      EventPipe.emit('showLoadingIcon');

      var nsName = $state.params.nsadmin;
      var path = '/configuration/user';

      dataSrc.request({
        _cdapPath: '/unrecoverable/namespaces/' + $state.params.nsadmin,
        method: 'DELETE'
      })
      .then(function () {
        myNamespace.getList(true).then(function() {
          EventPipe.emit('namespace.update');
        });

        dataSrc.request({
          _cdapPath: path
        }).then(function (res) {
          if (res.property) {
            // Delete Plugin Templates
            if (res.property.pluginTemplates) {
              delete res.property.pluginTemplates[nsName];
            }
            // Delete Hydrator Drafts
            if (res.property.adapterDrafts) {
              delete res.property.adapterDrafts[nsName];
            }
            // Delete Operation Dashboard
            if (res.property.dashsbyns) {
              delete res.property.dashsbyns[nsName];
            }
          }

          dataSrc.request({
            _cdapPath: path,
            method: 'PUT',
            body: res.property
          }).then(function () {
            // Success
            EventPipe.emit('hideLoadingIcon.immediate');

            $state.go('admin.overview', {}, {reload: true})
              .then(function () {
                myAlertOnValium.show({
                  type: 'success',
                  content: 'You have successfully deleted a namespace'
                });
              });


          });

        });

      }, function error() {
        EventPipe.emit('hideLoadingIcon.immediate');
      });
    };

  });
