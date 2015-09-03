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

angular.module(PKG.name + '.feature.home')
  .controller('HomeController', function ($state, rNsList, mySessionStorage, myLoadingService, $filter, EventPipe, StatusFactory) {
    StatusFactory.startPolling();
    // Needed to inject StatusFactory here for angular to instantiate the service and start polling.
    // check that $state.params.namespace is valid
    var n = rNsList.filter(function (one) {
      return one.name === $state.params.namespace;
    });

    function checkNamespace (ns) {
      var def = $filter('filter')(rNsList, { name: ns }, true);
      return def.length > 0 ? true : false;
    }

    var PREFKEY = 'feature.home.ns.latest';

    if(!n.length) {
      mySessionStorage.get(PREFKEY)
        .then(function (latest) {

          if (latest && checkNamespace(latest)) {
            $state.go($state.current, {namespace: latest}, {reload: true});
            return;
          }
          //check for default
          if (checkNamespace('default')){
            $state.go($state.current, {namespace: 'default'}, {reload: true});
            return;
          }

          // evoke backend is down
          EventPipe.emit('backendDown');

        });
    }
    else {
      mySessionStorage.set(PREFKEY, $state.params.namespace);
    }
    myLoadingService.hideLoadingIcon();
  });
