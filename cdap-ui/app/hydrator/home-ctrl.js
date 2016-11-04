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
  .controller('HydratorHomeController', function ($state, $stateParams, rNsList, mySessionStorage, myLoadingService) {

    if (!rNsList.length) {
      $state.go('unauthorized');
      return;
    }
    // Needed to inject StatusFactory here for angular to instantiate the service and start polling.
    // check that $state.params.namespace is valid

    //Access local storage for currently set namespace; if none is currently set resort to default ns

    let ns = $state.params.namespace;
    let defaultNS = localStorage.getItem('DefaultNamespace');
    let setNamespace = ns ? ns : defaultNS;

    var n = rNsList.filter(function (one) {
      return one.name === setNamespace;
    });

    function checkNamespace (ns) {
      return rNsList.filter(namespace => namespace.name === ns).length;
    }

    var PREFKEY = 'feature.home.ns.latest';

    if(!n.length) {
      mySessionStorage.get(PREFKEY)
        .then(function (latest) {

          if (latest && checkNamespace(latest)) {
            $state.go('hydrator.list', {namespace: latest}, {reload: true});
            return;
          }
          //check for default
          if (checkNamespace('default')){
            $state.go('hydrator.list', {namespace: 'default'}, {reload: true});
            return;
          } else {
            $state.go('hydrator.list', { namespace: rNsList[0].name }, { reload: true });
            return;
          }
        });
    }
    else {
      mySessionStorage.set(PREFKEY, $state.params.namespace);
      $state.go('hydrator.list', { namespace: setNamespace }, { reload: true});
    }
    myLoadingService.hideLoadingIcon();
  });
