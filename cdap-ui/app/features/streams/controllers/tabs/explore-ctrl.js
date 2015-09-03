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
  .controller('StreamExploreController', function($scope, $state, EventPipe, myStreamApi) {

    this.activePanel = [0];
    this.name = $state.params.streamId;

    var now = Date.now();

    this.eventSearch = {
      startMs: now-(60*60*1000*2), // two hours ago
      endMs: now,
      limit: 10,
      results: []
    };

    this.doEventSearch = function () {
      var params = {
        namespace: $state.params.namespace,
        streamId: $state.params.streamId,
        scope: $scope,
        start: this.eventSearch.startMs,
        end: this.eventSearch.endMs,
        limit: this.eventSearch.limit
      };
      myStreamApi.eventSearch(params)
        .$promise
        .then(function (res) {
          this.eventSearch.results = res;
        }.bind(this));
    };

    this.doEventSearch();

  });
