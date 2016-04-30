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
    this.schema = [];
    this.properties = {};
    this.activePropertyTab = 0;

    this.loading = true;

    metadataApi.then( (res) => {
      this.loading = false;
      this.processResponse(res);
    }, (err) => {
      this.loading = false;
      console.log('Error', err);
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

    this.properties = {
      system: systemMetadata.properties,
      user: userMetadata.properties,
      isUserEmpty: false,
      isSystemEmpty: false
    };

    /**
     * Need to show Dataset Spec from Dataset Properties if
     * dataset type is externalDataset. Ideally Backend should
     * return this automatically.
     **/
    if (systemMetadata.properties.type === 'externalDataset') {
      this.fetchExternalDatasetProperties();
    }

    if (Object.keys(userMetadata.properties).length === 0) {
      this.activePropertyTab = 1;
      this.properties.isUserEmpty = true;
    }

    this.properties.isSystemEmpty = Object.keys(systemMetadata.properties).length === 0;

    this.schema = this.parseSchema(systemMetadata.properties.schema);
  }

  fetchExternalDatasetProperties() {
    let datasetParams = {
      namespace: this.$state.params.namespace,
      entityId: this.$state.params.entityId,
      scope: this.$scope
    };
    this.myTrackerApi.getDatasetDetail(datasetParams)
      .$promise
      .then( (res) => {
        let datasetProperties = res.spec.properties;

        angular.extend(this.properties.user, datasetProperties);
        if (Object.keys(this.properties.user).length > 0) {
          this.activePropertyTab = 0;
          this.properties.isUserEmpty = false;
        }
      });
  }

  parseSchema(schema) {
    let jsonSchema;

    try {
      jsonSchema = JSON.parse(schema);
    } catch (e) {
      console.log('Error parsing schema JSON');
      return [];
    }

    let fieldsArr = [];
    angular.forEach(jsonSchema.fields, (field) => {
      let obj = {
        name: field.name
      };

      if (angular.isArray(field.type)) {
        obj.type = field.type[0];
        obj.null = true;
      } else {
        obj.type = field.type;
        obj.null = false;
      }

      if (angular.isObject(obj.type)) {
        obj.fullSchema = angular.copy(obj.type);
        obj.type = obj.type.type;
      }

      fieldsArr.push(obj);
    });

    return fieldsArr;
  }
}

TrackerMetadataController.$inject = ['$state', 'myTrackerApi', '$scope'];

angular.module(PKG.name + '.feature.tracker')
  .controller('TrackerMetadataController', TrackerMetadataController);
