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

    this.preferredTags = [
      {
        'key': 'tag1',
        'total_entity': 93
      },
      {
        'key': 'tag2',
        'total_entity': 0
      },
      {
        'key': 'tag3',
        'total_entity': 32
      },
      {
        'key': 'tag4',
        'total_entity': 32
      },
      {
        'key': 'tag5',
        'total_entity': 0
      }
    ];

    this.userTags = [
      {
        'key': 'tag1',
        'total_entity': 4
      },
      {
        'key': 'tag2',
        'total_entity': 1
      },
      {
        'key': 'tag3',
        'total_entity': 39
      },
      {
        'key': 'tag4',
        'total_entity': 17
      },
      {
        'key': 'tag5',
        'total_entity': 5
      }
    ];

    this.goToTag = function(event, tag) {
      event.stopPropagation();
      $state.go('search.objectswithtags', {tag: tag});
    };
  }

  addPreferredTags() {
    this.$uibModal.open({
      templateUrl: '/assets/features/tracker/templates/partial/add-preferred-tags-popover.html',
      size: 'md',
      backdrop: true,
      keyboard: true,
      windowTopClass: 'tracker-modal add-preferred-tags-modal'
    });
  }

}

TrackerTagsController.$inject = ['$state', '$scope', 'myTrackerApi', '$uibModal'];

angular.module(PKG.name + '.feature.tracker')
  .controller('TrackerTagsController', TrackerTagsController);
