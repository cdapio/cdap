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

angular.module(PKG.name + '.feature.admin').controller('NamespaceMetadataController',
function ($scope, $state, myAlert, MyDataSource, myNamespace) {

  $scope.nsname = myNamespace.getDisplayName($state.params.nsadmin);
  var data = new MyDataSource($scope);
  var path = '/namespaces/' + $state.params.nsadmin;

  data.request({
    _cdapPath: path
  })
    .then(function (metadata) {
      $scope.metadata = metadata;
    });

  $scope.doSave = function () {
    myAlert({
      title: 'it doesn\'t work yet',
      content: 'there is no content yet',
      type: 'warning'
    });
  };

});
