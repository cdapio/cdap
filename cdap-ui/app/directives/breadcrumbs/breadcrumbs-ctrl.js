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

angular.module(PKG.name + '.commons')
  .controller('breadcrumbsCtrl', function (MYAUTH_EVENT, myNamespace, $scope, EventPipe, $dropdown, $state) {
    // Namespace dropdown
    $scope.namespaces = [];
    $scope.hideNsDropdown = false;
    function updateNamespaceList() {
      myNamespace.getList()
        .then(function(list) {
          $scope.namespaces = list;
        });
    }
    updateNamespaceList();

    $scope.$watch('hideNsDropdown', function() {
      if (angular.isDefined($scope.hideNsDropdown)) {
        var toggles = $scope.element[0].querySelectorAll('a.ns-dropdown-toggle');
        var element;
         element = angular.element(toggles[0]);
        // namespace dropdown
        $dropdown(element, {
          template: 'breadcrumbs/namespace.html',
          animation: 'none',
          scope: $scope
        });
      }
    });

    // Listening for event from namespace create or namespace delete
    EventPipe.on('namespace.update', function() {
      updateNamespaceList();
    });

    $scope.$on (MYAUTH_EVENT.loginSuccess, updateNamespaceList);
    $scope.namespace = $state.params.namespace;
    $scope.$on (MYAUTH_EVENT.logoutSuccess, function () {
      $scope.namespaces = [];
    });

  });
