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
  .controller('MyPostRunActionWizardCtrl', function($scope, uuid, myAlertOnValium) {
    'ngInject';
    var vm = this;
    vm.action = vm.action || {};
    if (vm.isEdit) {
      vm.currentStage = 2;
      vm.selectedAction = Object.assign({}, vm.action.plugin, {
        defaultArtifact: vm.action.plugin.artifact
      });
    } else {
      vm.currentStage = 1;
    }
    vm.goToPreviousStep = function() {
      vm.currentStage -=1;
    };
    vm.onActionSelect = function(action) {
      vm.selectedAction = action;
      vm.currentStage = 2;
    };
    vm.onActionConfigure = function(action) {
      vm.configuredAction = action;
      vm.currentStage += 1;
    };
    vm.onActionConfirm = function(action) {
      vm.confirmedAction = {
        name: vm.action.name || action.name + uuid.v4(),
        plugin: {
          name: action.name,
          type: action.type,
          artifact: action.defaultArtifact,
          properties: action.properties
        }
      };
      try {
        if (vm.isEdit) {
          vm.actionCreator.editPostAction(vm.confirmedAction);
        } else {
          vm.actionCreator.addPostAction(vm.confirmedAction);
        }
        myAlertOnValium.show({
          type: 'success',
          content: vm.confirmedAction.plugin.name + ' post action saved.'
        });
      } catch(e) {
        console.log('ERROR', e);
        //FIXME: We should be able to handle errors more efficiently
      }
      $scope.$parent.$close();
    };
  });
