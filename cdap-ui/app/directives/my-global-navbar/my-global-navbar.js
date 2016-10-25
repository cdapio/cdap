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

angular.module(PKG.name+'.commons')
  .directive('caskHeaderActions', function(reactDirective) {
    return reactDirective(window.CaskCommon.HeaderActions);
  })
  .directive('caskHeaderBrand', function(reactDirective) {
    return reactDirective(window.CaskCommon.HeaderBrand);
  })
  .directive('myGlobalNavbar', () => {
    return {
      restrict: 'E',
      templateUrl: 'my-global-navbar/my-global-navbar.html',
      controller: function($scope, $state, myNamespace, EventPipe, myAuth) {
        var vm = this;
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
        if (window.CDAP_CONFIG.securityEnabled && myAuth.isAuthenticated()) {
          window.CaskCommon.Store.dispatch({
            type: 'UPDATE_USERNAME',
            payload: {
              username: myAuth.getUsername()
            }
          });
          vm.currentUser = myAuth.getUsername();
        }
        $scope.$on('$stateChangeSuccess', function(event, toState) {
          vm.highlightTab = toState.data && toState.data.highlightTab;
          if (!$state.params.namespace) {
            return;
          }
          if (window.CaskCommon && window.CaskCommon.Store) {
            window.CaskCommon.Store.dispatch({
              type: 'SELECT_NAMESPACE',
              payload: {
                selectedNamespace : $state.params.namespace
              }
            });
            window.CaskCommon.Store.dispatch({
              type: 'UPDATE_NAMESPACES',
              payload: {
                namespaces: vm.namespaces
              }
            });
          }
          vm.activeProduct = findActiveProduct();
        });
        vm.namespaces = [];
        function updateNamespaceList() {
          myNamespace.getList(true)
            .then(function(list) {
              vm.namespaces = list;
              window.CaskCommon.Store.dispatch({
                type: 'UPDATE_NAMESPACES',
                payload: {
                  namespaces: vm.namespaces
                }
              });
            });
        }
        EventPipe.on('namespace.update', updateNamespaceList);
      },
      controllerAs: 'Navbar'
    };
  });
