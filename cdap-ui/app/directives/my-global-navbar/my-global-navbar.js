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

function NavbarController ($scope, $state, myNamespace, EventPipe, MYAUTH_EVENT, myAuth, MY_CONFIG, $cookies, myHelpers) {
  'ngInject';

  let vm = this;

  vm.$cookies = $cookies;

  function findActiveProduct() {
    var baseTag, baseUrl;
    if ($state.is('userprofile')) {
      baseTag = document.getElementsByTagName('base');
      baseUrl = baseTag[0].getAttribute('href');
      if (baseUrl.indexOf('hydrator') !== -1) {
        return 'hydrator';
      } else if (baseUrl.indexOf('tracker') !== -1) {
        return 'tracker';
      } else {
        return 'cdap';
      }
    }
    if ($state.includes('hydrator.**')) {
      return 'hydrator';
    } else if ($state.includes('tracker.**') || $state.is('tracker-enable')) {
      return 'tracker';
    } else {
      return 'cdap';
    }
  }
  vm.showSidebar = false;
  vm.toggleSidebar = () => {
    vm.showSidebar = !vm.showSidebar;
  };
  vm.securityEnabled = MY_CONFIG.securityEnabled;
  vm.environment = MY_CONFIG.isEnterprise ? 'Distributed' : 'Standalone';

  $scope.$on('$stateChangeSuccess', function(event, toState) {
    vm.highlightTab = toState.data && toState.data.highlightTab;
    vm.activeProduct = findActiveProduct();
    vm.showSidebar = false;
    if (window.PlusButton && window.PlusButton.Store) {
      window.PlusButton.Store.dispatch({
        type: 'SELECT_NAMESPACE',
        payload: {
          selectedNamespace : $state.params.namespace
        }
      });
      window.PlusButton.Store.dispatch({
        type: 'UPDATE_NAMESPACES',
        payload: {
          namespaces: vm.namespaces
        }
      });
    }
  });

  // NAMESPACE
  vm.namespaces = [];
  function updateNamespaceList() {
    myNamespace.getList(true)
      .then(function(list) {
        vm.namespaces = list;
      });
  }
  // Listening for event from namespace create or namespace delete
  EventPipe.on('namespace.update', updateNamespaceList);

  vm.currentUser = myAuth.getUsername();
  $scope.$on (MYAUTH_EVENT.loginSuccess, () => {
    vm.currentUser = myAuth.getUsername();
    updateNamespaceList();
  });
  $scope.$on (MYAUTH_EVENT.logoutSuccess, () => {
    vm.currentUser = myAuth.getUsername();
    vm.namespaces = [];
  });

  vm.logout = myAuth.logout.bind(myAuth);
  vm.changeNamespace = (ns) => {
    if ($state.params.namespace === ns.name) { return; }

    $cookies.put('CDAP_Namespace', ns.name);

    if ($state.includes('hydrator.**')) {
      $state.go('hydrator.list', { namespace: ns.name });
    } else if ($state.includes('tracker.**') || $state.is('tracker-enable')) {
      $state.go('tracker.home', { namespace: ns.name });
    } else if ($state.includes('dashboard.**')){
      $state.go('dashboard.standard.cdap', { namespace: ns.name });
    } else {
      $state.go('overview', { namespace: ns.name });
    }
  };

  vm.getAbsUIUrl = myHelpers.getAbsUIUrl;
  $scope.$on('$destroy', () => {
    $cookies.remove('CDAP_Namespace');
  });

}


angular.module(PKG.name+'.commons')
  .directive('plusButton', function(reactDirective) {
    return reactDirective(window.PlusButton.default);

  })
  .directive('myGlobalNavbar', () => {
    return {
      restrict: 'E',
      templateUrl: 'my-global-navbar/my-global-navbar.html',
      controller: NavbarController,
      controllerAs: 'Navbar'
    };
  });
