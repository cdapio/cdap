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

angular.module(PKG.name + '.feature.admin').controller('NamespaceAppController',
function ($scope, $state, myAppUploader, MyDataSource, myNamespace, myAdapterApi, $alert, GLOBALS, myAdapterFactory) {

  $scope.apps = [];
  $scope.GLOBALS = GLOBALS;
  $scope.myAdapterFactory = myAdapterFactory;
  $scope.nsname = myNamespace.getDisplayName($state.params.nsadmin);

  var myDataSrc = new MyDataSource($scope);
  var path = '/namespaces/' + $state.params.nsadmin + '/apps';
  myDataSrc.request({
    _cdapPath: path
  })
    .then(function(response) {
      $scope.apps = $scope.apps.concat(response);
    });

  $scope.deleteApp = function deleteApp(id) {
    myDataSrc.request({
      _cdapPath: path + '/' + id,
      method: 'DELETE'
    }, function() {
      $alert({
        type: 'success',
        title: id,
        content: 'App deleted successfully'
      });
      $state.reload();
    });
  };

  $scope.onFileSelected = function(files) {
    myAppUploader.upload(files, $state.params.nsadmin);
  };
});
