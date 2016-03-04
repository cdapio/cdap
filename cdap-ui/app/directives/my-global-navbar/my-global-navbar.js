/*
 * Copyright © 2016 Cask Data, Inc.
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

function NavbarController ($scope, $state) {
  'ngInject';

  let vm = this;

  function findActiveProduct() {
    if ($state.includes('hydrator.**')) {
      return 'hydrator';
    } else if ($state.includes('tracker.**') || $state.is('tracker-home')) {
      return 'tracker';
    } else {
      return 'cdap';
    }
  }

  vm.showSidebar = false;

  vm.toggleSidebar = () => {
    vm.showSidebar = !vm.showSidebar;
  };

  $scope.$on('$stateChangeSuccess', function(event, toState) {
    vm.highlightTab = toState.data && toState.data.highlightTab;
    vm.activeProduct = findActiveProduct();
    vm.showSidebar = false;
  });

}


angular.module(PKG.name+'.commons')
  .directive('myGlobalNavbar', () => {
    return {
      restrict: 'E',
      templateUrl: 'my-global-navbar/my-global-navbar.html',
      controller: NavbarController,
      controllerAs: 'Navbar'
    };
  });
