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

class TrackerTagsController{
  constructor($state, $scope, myTrackerApi, $uibModal) {
    this.$state = $state;
    this.$scope = $scope;
    this.myTrackerApi = myTrackerApi;
    this.$uibModal = $uibModal;
    this.currentPreferredPage = 1;
    this.currentUserPage = 1;
    this.loadingTags = true;
    this.tags = {
      preferredTags: [],
      userTags: []
    };
    this.fetchTags();
  }

  fetchTags() {
    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope
    };

    this.myTrackerApi.getTags(params)
      .$promise
      .then((response) => {
        const getTags = (tagsObj) => {
           return Object.keys(tagsObj)
            .map(tag => ({
              name: tag,
              count: tagsObj[tag]
            }));
        };
       this.tags.preferredTags = getTags(response.preferredTags);
        // Calculate number of empty rows needed so that table maintains a consistent height
        if ((this.tags.preferredTags.length % 10) !== 0) {
          let emptyRows = 10 - (this.tags.preferredTags.length % 10);
          this.tags.emptyPreferredTags = {
            emptyArr: Array(emptyRows),
            numPages: Math.ceil(this.tags.preferredTags.length / 10)
          };
        }

        this.tags.userTags = getTags(response.userTags);
        // Calculate number of empty rows needed so that table maintains a consistent height
        if ((this.tags.userTags.length % 10) !== 0) {
          let emptyRows = 10 - (this.tags.userTags.length % 10);
          this.tags.emptyUserTags = {
            emptyArr: Array(emptyRows),
            numPages: Math.ceil(this.tags.userTags.length / 10)
          };
        }
        this.loadingTags = false;
      }, (err) => {
        this.loadingTags = false;
        console.log('Error', err);
      });
  }

  promoteUserTag(tag) {
    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope
    };

    this.myTrackerApi.promoteUserTags(params, [tag])
      .$promise
      .then((response) => {
        this.response = response;
        this.fetchTags();
      }, (err) => {
        console.log('Error', err);
      });
  }

  demotePreferredTag(tag) {
    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope
    };

    this.myTrackerApi.demotePreferredTags(params, [tag])
      .$promise
      .then((response) => {
        this.response = response;
        this.fetchTags();
      }, (err) => {
        console.log('Error', err);
      });
  }

  showDeleteModal(tag) {
    let modal = this.$uibModal.open({
      templateUrl: '/assets/features/tracker/templates/partial/delete-preferred-tags-popover.html',
      size: 'sm',
      backdrop: true,
      keyboard: true,
      windowTopClass: 'tracker-modal delete-modal',
      controller: DeletePreferredTagsModalCtrl,
      controllerAs: 'DeleteTags',
      resolve: {
        tag: () => {
          return tag;
        }
      }
    });
    modal.result
      .then((message) => {
        if (message === 'success') {
          this.fetchTags();
        }
    });
  }

  showAddModal() {
    let modal = this.$uibModal.open({
      templateUrl: '/assets/features/tracker/templates/partial/add-preferred-tags-popover.html',
      size: 'sm',
      backdrop: true,
      keyboard: true,
      windowTopClass: 'tracker-modal add-preferred-tags-modal',
      controller: AddPreferredTagsModalCtrl,
      controllerAs: 'AddTags'
    });
    modal.result
      .then((message) => {
        if (message === 'success') {
          this.fetchTags();
        }
    });
  }
}

function AddPreferredTagsModalCtrl (myTrackerApi, $scope, $state) {
  'ngInject';
  this.proceedToNextStep = false;

  function parseTags(tags) {
    let parsedTags = [];
    if(!tags) {
      return parsedTags;
    }
    angular.forEach(tags.split('\n'), (line) => {
      if (!line) {
        return;
      }
      parsedTags = parsedTags.concat(line.split(',').map((tag) => {
        return tag.trim();
      }));
    });

    return parsedTags;
  }
  this.validatePreferredTags = () => {
    let tags = parseTags(this.tags);

    let validateParams = {
      namespace: $state.params.namespace,
      scope: $scope
    };
    myTrackerApi.validatePreferredTags(validateParams, tags)
      .$promise
      .then((response) => {
        this.tagList = response;
        if (this.tagList.validTags.length > 0) {
          this.proceedToNextStep = true;
        }
      }, (err) => {
        console.log('Error', err);
      });
  };
  this.promoteUserTags = (tags) => {
    let addParams = {
      namespace: $state.params.namespace,
      scope: $scope
    };
    myTrackerApi.promoteUserTags(addParams, tags)
      .$promise
      .then(() => {
        $scope.$close('success');
      }, (err) => {
        console.log('Error', err);
      });
  };
  this.importTags = () => {
    document.getElementById('file-select').click();
  };
  this.importFiles = (files) => {
    if (files[0].name.indexOf('.txt') === -1 && files[0].name.indexOf('.csv') === -1) {
      this.invalidFormat = true;
      return;
    }

    let reader = new FileReader();
    reader.readAsText(files[0], 'UTF-8');
    this.invalidFormat = false;
    reader.onload = (evt) => {
      let data = evt.target.result;
      this.tags = data;
    };
  };
}

function DeletePreferredTagsModalCtrl (tag, myTrackerApi, $scope, $state, myAlertOnValium) {
  'ngInject';
  this.tag = tag;
  this.deletePreferredTags = () => {
    let deleteParams = {
      namespace: $state.params.namespace,
      scope: $scope,
      tag: tag
    };
    myTrackerApi.deletePreferredTags(deleteParams)
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

TrackerTagsController.$inject = ['$state', '$scope', 'myTrackerApi', '$uibModal'];

angular.module(PKG.name + '.feature.tracker')
  .controller('TrackerTagsController', TrackerTagsController);
