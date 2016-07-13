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

angular.module(PKG.name + '.commons')
  .directive('myPostRunActions', function() {
    return {
      restrict: 'E',
      scope: {
        actionCreator: '=',
        store: '=',
        isDisabled: '@'
      },
      templateUrl: 'my-post-run-actions/my-post-run-actions.html',
      bindToController: true,
      controller: ['$scope', 'myPostRunActionWizardService', 'myAlertOnValium', function($scope, myPostRunActionWizardService, myAlertOnValium) {
        this._isDisabled = this.isDisabled === 'true';
        $scope.myPostRunActionWizardService = myPostRunActionWizardService;
        var sub = this.store.registerOnChangeListener(() => {
          this.actions = this.store.getPostActions();
        });
        this.actions = this.store.getPostActions();
        this.deletePostRunAction = function(action) {
          this.actionCreator.deletePostAction(action);
          myAlertOnValium.show({
            type: 'success',
            content: action.plugin.name + ' post action deleted.'
          });
        };
        $scope.$on('$destroy', () => {
          sub();
        });
      }],
      controllerAs: 'MyPostRunActionsCtrl'
    };
  });
