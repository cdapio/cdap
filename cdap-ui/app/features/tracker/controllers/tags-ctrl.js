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
        this.tags = {
          preferredTags: [],
          userTags: []
        };

        angular.forEach(response.preferredTags, (count, tag) => {
          this.tags.preferredTags.push({
            name: tag,
            count: count
          });
        });

        angular.forEach((response.userTags), (count, tag) => {
          this.tags.userTags.push({
            name: tag,
            count: count
          });
        });
        console.log('this.tags.preferredTags', this.tags.preferredTags);
        console.log('this.tags.userTags', this.tags.userTags);
      }, (err) => {
        console.log('Error', err);
      });
  }

  promoteUserTags(tag) {
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

  demotePreferredTags(tag) {
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
      size: 'md',
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
      size: 'md',
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
  this.validatePreferredTags = () => {
    let tags = [];

    angular.forEach(this.tags.split('\n'), (line) => {
      tags = tags.concat(line.split(',').map((tag) => {
        return tag.trim();
      }));
    });

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
      .then((response) => {
        $scope.$close('success');
      }, (err) => {
        console.log('Error', err);
      });
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
      .then((response) => {
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
