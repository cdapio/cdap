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
  .controller('AdminController', function ($scope, $state, myNamespace) {

    myNamespace.getList()
      .then(function(list) {
        $scope.nsList = list;
        handleSubmenus();
      });

    // whether or not to show submenus
    $scope.submenu = {
      system: true,
      security: false,
      namespaces: true,
      perNs: {}
    };

    $scope.$on('$stateChangeSuccess', handleSubmenus);

    function handleSubmenus() {
      if (!$scope.submenu.security) {
        $scope.submenu.security = $state.is('admin.security') || $state.includes('admin.security.**');
      }
      if (!$scope.submenu.system) {
        $scope.submenu.system = $state.is('admin.system') || $state.includes('admin.system.**');
      }

      if (!$scope.submenu.namespaces) {
        $scope.submenu.namespaces = $state.is('admin.namespace') || $state.includes('admin.namespace.**');
      }

      if($state.params.nsadmin) {
        $scope.submenu.perNs[$state.params.nsadmin] = true;
      }
    }


  });
