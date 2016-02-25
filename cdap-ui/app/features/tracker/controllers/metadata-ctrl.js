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

class TrackerMetadataController{
  constructor($state, myTrackerApi, $scope) {
    this.$state = $state;
    this.myTrackerApi = myTrackerApi;
    this.$scope = $scope;

    let entitySplit = this.$state.params.entityType.split(':');

    let params = {
      scope: this.$scope,
      namespace: this.$state.params.namespace,
      entityType: entitySplit[0],
    };

    let metadataApi;

    if (entitySplit.length > 1) {
      params.entityId = entitySplit[1];
      params.entityType = 'streams';
      params.viewId = this.$state.params.entityId;
      metadataApi = this.myTrackerApi.viewsProperties(params).$promise;
    } else {
      params.entityId = this.$state.params.entityId;
      params.entityType = entitySplit[0];
      metadataApi = this.myTrackerApi.properties(params).$promise;
    }

    this.tags = {};

    metadataApi.then( (res) => {
      this.tempResult = res;

      this.processResponse(res);
    });

  }

  processResponse(res) {
    let systemMetadata, userMetadata;

    angular.forEach(res, (response) => {
      if (response.scope === 'USER') {
        userMetadata = response;
      } else if (response.scope === 'SYSTEM'){
        systemMetadata = response;
      }
    });

    this.tags = {
      system: systemMetadata.tags,
      user: userMetadata.tags
    };


  }
}

TrackerMetadataController.$inject = ['$state', 'myTrackerApi', '$scope'];

angular.module(PKG.name + '.feature.tracker')
  .controller('TrackerMetadataController', TrackerMetadataController);
