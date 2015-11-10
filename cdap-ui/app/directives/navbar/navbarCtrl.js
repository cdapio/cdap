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
  .controller('navbarCtrl', function (myAlert, MYAUTH_EVENT, myNamespace, $scope, EventPipe, $state) {

    $scope.namespaces = [];

    function updateNamespaceList() {
      myNamespace.getList()
        .then(function(list) {
          $scope.namespaces = list;
        });
    }

    // Listening for event from namespace create or namespace delete
    EventPipe.on('namespace.update', function() {
      updateNamespaceList();
    });

    $scope.$on (MYAUTH_EVENT.loginSuccess, updateNamespaceList);
    $scope.getDisplayName = myNamespace.getDisplayName.bind(myNamespace);

    $scope.$on (MYAUTH_EVENT.logoutSuccess, function () {
      $scope.namespaces = [];
    });

    $scope.highlightTab = $state.current.data && $state.current.data.highlightTab;
    $scope.$on('$stateChangeSuccess', function(event, toState) {
      $scope.highlightTab = toState.data && toState.data.highlightTab;
    });

    $scope.doSearch = function () {
      myAlert({
        title: 'Sorry',
        content: 'Search is not yet implemented.',
        type: 'danger'
      });
    };
  });
