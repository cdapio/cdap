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

angular.module(PKG.name + '.feature.streams')
  .controller('CdapStreamDetailController', function($scope, $state, $alert, myStreamService, myStreamApi) {

    this.truncate = function() {
      var params = {
        namespace: $state.params.namespace,
        streamId: $state.params.streamId,
        scope: $scope
      };
      myStreamApi.truncate(params, {})
        .$promise
        .then(function () {
          $alert({
            type: 'success',
            content: 'Successfully truncated ' + $state.params.streamId + ' stream'
          });
        });
    };

    this.sendEvents = function() {
      myStreamService.show($state.params.streamId);
    };

  });
