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

/**
 * This class is responsible for controlling the Metadata View in Tracker
 * entity detai page.
 **/
class TrackerMetadataController {
  constructor($state, myTrackerApi, $scope, myAlertOnValium, $timeout) {
    this.$state = $state;
    this.myTrackerApi = myTrackerApi;
    this.$scope = $scope;
    this.myAlertOnValium = myAlertOnValium;
    this.$timeout = $timeout;

    this.propertyInput = {
      key: '',
      value: ''
    };

    let entitySplit = this.$state.params.entityType.split(':');

    this.entityType = entitySplit;

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

    this.schema = systemMetadata.properties.schema;
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

  /* METADATA PROPERTIES CONTROL */
  /*
    TODO:
      - Add support for Stream Views
      - What to do with externalDataset type
  */
  enableAddProperty() {
    this.addPropertyEnable = true;
    this.propertyFocus();
  }

  deleteProperty(key) {
    let deleteParams = {
      namespace: this.$state.params.namespace,
      entityType: this.$state.params.entityType,
      entityId: this.$state.params.entityId,
      key: key,
      scope: this.$scope
    };
    this.myTrackerApi.deleteEntityProperty(deleteParams)
      .$promise
      .then(() => {
        delete this.properties.user[key];
      }, (err) => {
        this.myAlertOnValium.show({
          type: 'danger',
          content: err.data
        });
      });
  }

  addProperty() {
    if (!this.propertyInput.key || !this.propertyInput.value) { return; }

    let addParams = {
      namespace: this.$state.params.namespace,
      entityType: this.$state.params.entityType,
      entityId: this.$state.params.entityId,
      scope: this.$scope
    };

    let obj = {};
    obj[this.propertyInput.key] = this.propertyInput.value;

    this.myTrackerApi.addEntityProperty(addParams, obj)
      .$promise
      .then(() => {
        this.properties.user[this.propertyInput.key] = this.propertyInput.value;

        this.propertyInput.key = '';
        this.propertyInput.value = '';
        this.propertyFocus();
      }, (err) => {
        this.myAlertOnValium.show({
          type: 'danger',
          content: err.data
        });
      });
  }

  propertyKeypress(event) {
    switch (event.keyCode) {
      case 13: // Enter Key
        this.addProperty();
        break;
      case 27: // Esc key
        this.addPropertyEnable = false;
    }
  }

  propertyFocus() {
    this.$timeout( () => {
      let elem = document.getElementById('property-key-input');
      angular.element(elem)[0].focus();
    });
  }
}

TrackerMetadataController.$inject = ['$state', 'myTrackerApi', '$scope', 'myAlertOnValium', '$timeout'];

angular.module(PKG.name + '.feature.tracker')
  .controller('TrackerMetadataController', TrackerMetadataController);
