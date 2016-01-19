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

angular.module(PKG.name + '.feature.admin').controller('NamespaceAppMetadataController',
function ($scope, $state, myAlertOnValium, $timeout, MyCDAPDataSource, myHydratorFactory) {

  var data = new MyCDAPDataSource($scope);
  var path = '/namespaces/' + $state.params.nsadmin + '/apps/' + $state.params.appId;
  $scope.myHydratorFactory = myHydratorFactory;

  data.request({
    _cdapPath: path
  })
    .then(function(app) {
      $scope.app = app;
    });

  $scope.deleteApp = function(app) {
    data.request({
      _cdapPath: path,
      method: 'DELETE'
    }, function() {
      myAlertOnValium.show({
        type: 'success',
        title: app,
        content: 'Application deleted successfully'
      });
      // FIXME: Have to avoid $timeout here. Un-necessary.
      $timeout(function() {
        $state.go('^.apps');
      });
    });
  };

});
