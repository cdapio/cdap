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

angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorHomeController', function ($state, $stateParams, rNsList, mySessionStorage, myLoadingService, $window) {

    if (!rNsList.length) {
      $state.go('unauthorized');
      return;
    }
    // Needed to inject StatusFactory here for angular to instantiate the service and start polling.
    // check that $state.params.namespace is valid

    // Access local storage for currently set namespace; if none is currently set resort to default ns

    let ns = $state.params.namespace;
    let defaultNS = localStorage.getItem('DefaultNamespace');
    const namespaceToUse = ns ? ns : defaultNS;

    const validNamespace = rNsList.find(namespace => namespace.name === namespaceToUse);

    function checkNamespace (ns) {
      return rNsList.filter(namespace => namespace.name === ns).length;
    }

    var PREFKEY = 'feature.home.ns.latest';

    if (validNamespace) {
      mySessionStorage.get(PREFKEY)
        .then(function (latest) {
          let ns;

          if (latest && checkNamespace(latest)) {
            ns = latest;
          } else if (checkNamespace('default')) {  // check for default
            ns = 'default';
          } else {
            ns = rNsList[0].name;
          }

          $window.location.href = $window.getHydratorUrl({
            stateName: 'hydrator.list',
            stateParams: {
              namespace: ns,
            },
          });
        });
    }
    else {
      mySessionStorage.set(PREFKEY, $state.params.namespace);
      $window.location.href = $window.getHydratorUrl({
        stateName: 'hydrator.list',
        stateParams: {
          namespace: namespaceToUse
        },
      });
    }
    myLoadingService.hideLoadingIcon();
  });
