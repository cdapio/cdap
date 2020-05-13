/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
  .controller('MyPostRunActionWizardCtrl', function($scope, uuid) {
    'ngInject';
    var vm = this;
    vm.action = vm.action || {};
    if (vm.action && Object.keys(vm.action).length > 1) {
      vm.selectedAction = Object.assign({}, angular.copy(vm.action.plugin), {
        defaultArtifact: vm.action.plugin.artifact,
        id: vm.action.id,
        description: vm.action.description
      });
    } else {
      vm.selectedAction = {};
    }

    if (vm.mode === 'edit') {
      vm.currentStage = 2;
    } else if (vm.mode === 'view') {
      vm.currentStage = 3;
      vm.configuredAction = vm.selectedAction;
    } else if(vm.mode === 'create') {
      vm.currentStage = 1;
    }
    vm.goToPreviousStep = function() {
      vm.currentStage -=1;
      if (vm.currentStage === 1) {
        vm.selectedAction = {};
        $scope.$parent.action = vm.selectedAction;
      }
    };
    vm.onActionSelect = function(action) {
      vm.selectedAction = action;
      $scope.$parent.action = vm.selectedAction;
      vm.currentStage = 2;
    };
    vm.onActionConfigure = function(action, actionCallback) {
      var fn = vm.validate();
      const callback = (errors) => {
        if (typeof actionCallback === 'function') {
          actionCallback();
        }
        if (errors && Object.keys(errors).length) {
          return;
        }
        vm.configuredAction = action;
        $scope.$parent.action = vm.configuredAction;
        vm.currentStage += 1;
      };
      if ('undefined' !== typeof fn) {
        fn.call(null, action, callback, true);
      }
    };
    vm.onActionConfirm = function(action) {
      if (!action) {
        $scope.$parent.$close();
        return;
      }
      vm.confirmedAction = {
        name: vm.action.name || action.name,
        id: vm.action.id || action.name + uuid.v4(),
        plugin: {
          name: action.name,
          type: action.type,
          artifact: action.defaultArtifact,
          properties: action.properties
        },
        description: vm.action.description
      };
      try {
        if (vm.mode === 'edit') {
          vm.actionCreator.editPostAction(vm.confirmedAction);
        } else {
          vm.actionCreator.addPostAction(vm.confirmedAction);
        }
      } catch(e) {
        console.log('ERROR', e);
        //FIXME: We should be able to handle errors more efficiently
      }
      $scope.$parent.$close();
    };
  });
