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
class TrackerDictionaryController {
  constructor($state, $scope, myTrackerApi, $uibModal, myAlertOnValium) {
    this.$state = $state;
    this.$scope = $scope;
    this.myTrackerApi = myTrackerApi;
    this.$uibModal = $uibModal;
    this.myAlertOnValium = myAlertOnValium;
    this.currentPreferredPage = 1;
    this.currentUserPage = 1;
    this.addNewColumn = false;
    this.dictionaryData = [];
    this.piiTitle = 'PII (Personally Identifiable Information) is any information that is sensitive and needs to be handled with extra care and security.';
    this.operation = true;
    this.fetchDictionary();
    this.selectedDictColumn = {
      'columnType': '',
      'isNullable': true,
      'isPII': true,
      'description': ''
    };
  }

  fetchDictionary() {
    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope
    };

    this.myTrackerApi.getDictionary(params)
      .$promise
      .then((response) => {
        this.dictionaryData = response;
      }, (err) => {
        console.log('Error', err);
    });
  }

  getTemplate(data) {
    if (data.columnName === this.selectedDictColumn.columnName && !this.addNewColumn) {
      return '/assets/features/tracker/templates/partial/dictionary-row-edit.html';
    } else {
      return '/assets/features/tracker/templates/partial/dictionary-row-display.html'; 
    }
  }

  editColumnData(data) {
    this.reset();
    this.selectedDictColumn = data;
  }

  reset() {
    this.addNewColumn = false;
    this.selectedDictColumn = {};
  }

  displayNewRow() {
    this.reset();
    this.addNewColumn = true;
  }

  addColumn() {
    if(this.selectedDictColumn.columnName && this.selectedDictColumn.columnType && this.selectedDictColumn.description){
      let addParams = {
        namespace: this.$state.params.namespace,
        scope: this.$scope,
        columnName: this.selectedDictColumn.columnName
      };
      this.myTrackerApi.addColumn(addParams, this.selectedDictColumn)
        .$promise
        .then(() => {
          this.addNewColumn = false;
          this.reset();
          this.fetchDictionary();
        }, (err) => {
          this.myAlertOnValium.show({
            type: 'danger',
            content: err.data
          });
          console.log('Error', err);
      });
    }
  }

  updateColumn() {
    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope,
      columnName: this.selectedDictColumn.columnName
    };

    this.myTrackerApi.updateColumn(params, this.selectedDictColumn)
      .$promise
      .then((response) => {
        this.response = response;
      }, (err) => {
        console.log('Error', err);
    });
  }

  showDeleteModal(columnName) {
    let modal = this.$uibModal.open({
      templateUrl: '/assets/features/tracker/templates/partial/delete-column-from-dictionary.html',
      size: 'sm',
      backdrop: true,
      keyboard: true,
      windowTopClass: 'tracker-modal delete-modal',
      controller: deleteFromDictionary,
      controllerAs: 'DeleteColumns',
      resolve: {
        columnName: () => {
          return columnName;
        }
      }
    });
    modal.result
      .then((message) => {
        if (message === 'success') {
          this.fetchDictionary();
        }
      });
    }
  }

  function deleteFromDictionary(columnName, myTrackerApi, $scope, $state, myAlertOnValium) {
    'ngInject';
    this.columnName = columnName;
    this.deleteColumn = () => {
      let deleteParams = {
        namespace: $state.params.namespace,
        scope: $scope,
        columnName: columnName
      };
      myTrackerApi.deleteColumn(deleteParams)
        .$promise
        .then(() => {
          $scope.$close('success');
        }, (err) => {
          myAlertOnValium.show({
            type: 'danger',
            content: err.data
          });
      });
    };
  }

TrackerDictionaryController.$inject = ['$state', '$scope', 'myTrackerApi', '$uibModal', 'myAlertOnValium'];

angular.module(PKG.name + '.feature.tracker')
    .controller('TrackerDictionaryController', TrackerDictionaryController);
