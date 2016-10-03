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

class MyLinkButtonCtrl {
  constructor($stateParams) {
    this.entities.forEach(entity => {

      if (entity.entityType === 'streams') {
        entity.url = window.getTrackerUrl({
          stateName: 'tracker.detail.entity.metadata',
          stateParams: {
            namespace: $stateParams.namespace,
            entityType: 'streams',
            entityId: entity.streamId
          }
        });
      } else {
        entity.url = window.getTrackerUrl({
          stateName: 'tracker.detail.entity.metadata',
          stateParams: {
            namespace: $stateParams.namespace,
            entityType: 'datasets',
            entityId: entity.datasetId
          }
        });
      }

    });
  }
}
MyLinkButtonCtrl.$inject = ['$stateParams'];

angular.module(PKG.name + '.commons')
  .directive('myLinkButton', function() {
    return {
      restrict: 'E',
      scope: {
        entities: '='
      },
      bindToController: true,
      controller: MyLinkButtonCtrl,
      controllerAs: 'MyLinkButtonCtrl',
      templateUrl: 'my-link-button/my-link-button.html'
    };
  });
