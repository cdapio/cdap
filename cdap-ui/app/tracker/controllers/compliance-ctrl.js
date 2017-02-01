/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

class TrackerComplianceController{
  constructor($state, myTrackerApi, $scope, avsc, SchemaHelper, $q) {
    this.$state = $state;
    this.$q = $q;
    this.myTrackerApi = myTrackerApi;
    this.$scope = $scope;
    this.SchemaHelper = SchemaHelper;
    this.formattedData = [];
    this.complincePassed = false;
    this.loading = true;
    this.noSchema = false;
    this.avsc = avsc;
    this.complianceCheck();
  }

  complianceCheck() {
    let entitySplit = this.$state.params.entityType.split(':');

    let params = {
      scope: this.$scope,
      namespace: this.$state.params.namespace,
      entityType: entitySplit[0],
    };

    if (entitySplit.length > 1) {
      params.entityId = entitySplit[1];
      params.entityType = 'streams';
      params.viewId = this.$state.params.entityId;
    } else {
      params.entityId = this.$state.params.entityId;
      params.entityType = entitySplit[0];
    }

    this.displayEntityType = params.entityType.slice(0, -1);
    let metadataApi = this.myTrackerApi.properties(params).$promise;

    metadataApi.then((res) => {
      this.processDataResponse(res);
    }, (err) => {
      console.log('Error', err);
    });
  }

  processDataResponse(res) {
    let systemMetadata;

    angular.forEach(res, (response) => {
      if (response.scope === 'SYSTEM') {
        systemMetadata = response;
      }
    });

    this.schema = systemMetadata.properties.schema;
    let params1 = {
      namespace: this.$state.params.namespace,
      scope: this.$scope
    };
    if (!this.schema) {
      this.noSchema = true;
    }

    let parsed = this.avsc.parse(this.schema, { wrapUnions: true });
    this.parsedSchema = parsed.getFields().map((field) => {
      let type = field.getType();
      let partialObj = this.SchemaHelper.parseType(type);
      return Object.assign({}, partialObj, {
        id: uuid.v4(),
        name: field.getName()
      });
    });

    let listOfPromises = [];
    this.parsedSchema.filter((obj) => {
      if (obj){
        let dictionaryObj = {
          'columnName': obj.name,
          'columnType': obj.displayType,
          'isNullable': obj.nullable
        };

        listOfPromises.push(this.myTrackerApi.complianceCheck(params1, dictionaryObj).$promise.then(() => {}, (err) => {
          let dataObject = {};
          dataObject.columnName = dictionaryObj.columnName;
          dataObject.columnType = dictionaryObj.columnType;
          dataObject.isNullable = dictionaryObj.isNullable;
          dataObject.isPII = err.data.isPII;
          dataObject.statusCode = err.statusCode;
          dataObject.reason = [];

          if (err.statusCode === 409) {
            if (err.data.columnType !== err.data.expectedType) {
              dataObject.reason.push('This field should have type ' + err.data.expectedType);
            }
            if (err.data.isNullable !== err.data.expectedNullable) {
              if (err.data.expectedNullable) {
                dataObject.reason.push('This field should be marked as Nullable');
              } else {
                dataObject.reason.push('This field should not be marked as Nullable');
              }
            }
            if (err.data.isPII !== err.data.expectedPII) {
              if (err.data.expectedPII) {
                dataObject.reason.push('This field should be marked PII');
              } else {
                dataObject.reason.push('This field should not be marked PII');
              }
            }
          }
          if (err.statusCode === 404) {
            dataObject.reason.push('Add to Data Dictionary');
          }

          this.formattedData.push(dataObject);
        }));
      }
    });

    this.$q.all(listOfPromises)
      .then(() => {
        this.loading = false;
      }, (err) => {
        console.log(err);
    });
  }
}

TrackerComplianceController.$inject = ['$state', 'myTrackerApi', '$scope','avsc', 'SchemaHelper', '$q'];

angular.module(PKG.name + '.feature.tracker')
 .controller('TrackerComplianceController', TrackerComplianceController);
