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
  .directive('myJsonTextbox', function() {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel',
        placeholder: '=',
        disabled: '='
      },
      controllerAs: 'JsonEditor',
      bindToController: true,
      templateUrl: 'widget-container/widget-json-editor/widget-json-editor.html',
      controller: function($scope) {
        var vm = this;
        vm.warning = null;

        function attemptParse(input) {
          try {
            let parsedModel = angular.fromJson(input);

            vm.internalModel = angular.toJson(parsedModel, true);
            vm.warning = null;
          } catch (e) {
            vm.internalModel = input;
            vm.warning = 'Value is not a JSON';
          }
        }

        function initialize () {
          attemptParse(vm.model);
        }

        initialize();

        vm.tidy = function() {
          attemptParse(vm.internalModel);
        };

        $scope.$watch('JsonEditor.internalModel', function(oldVal, newVal) {
          if (oldVal !== newVal) {
            // removing newlines
            try {
              let parsed = angular.fromJson(vm.internalModel);
              vm.model = angular.toJson(parsed);
            } catch (e) {
              vm.model = vm.internalModel;
            }
          }
        });
      }
    };
  });
