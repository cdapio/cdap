/*
 * Copyright © 2017 - 2018 Cask Data, Inc.
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

function InputValiadtorController($scope) {
  var vm = this;

  vm.showErrorMessage = false;
  // vm.value = '';

  $scope.$on('$onInit', () => {
    // this.value = this.inputValue;
  });


  vm.getInputInfoMessage = () => {
    if(vm.tooltip && vm.tooltip !== undefined) {
      return vm.tooltip + '\n Cannot  contain any xml tags.';
    } else {
      return 'Cannot  contain any xml tags.';
    }
  };

  vm.getErrorMessage = () => {
    return 'Invalid Input, see help.';
  };

  vm.onValueChange = () => {
    if(vm.model !== undefined) {
      vm.showErrorMessage = vm.isValidValue(vm.model) ? false : true;
    }
  };

  $scope.$watch('model', function() {
    if(vm.model !== undefined && vm.model.trim() !==  '') {
      vm.showErrorMessage = vm.isValidValue(vm.model) ? false : true;
    }
  });


  // vm.onValueChange = () => {
  //   if (vm.isValidValue(vm.value)) {
  //     vm.inputValue = vm.value;
  //     vm.showErrorMessage = false;
  //   } else {
  //     vm.inputValue = '';
  //     vm.showErrorMessage = true;
  //   }
  // };

  vm.isValidValue = (dirty) => {
    var allowed = {
      ALLOWED_TAGS: [],
    };

    const clean = window['DOMPurify'].sanitize(dirty, allowed);
    return clean === dirty ? true : false;
  };
}


angular.module(PKG.name + '.commons')
  .directive('inputValidator', function () {
    return {
      restrict: 'E',
      replace: true,
      templateUrl: 'input-validator/input-validator.html',
      bindToController: true,
      scope: {
        tooltip: '@',
        errorMessage: '@',
        placeholder: '@',
        //inputValue: '=',
        disabled: '=',
        model: '='
      },
      controller: InputValiadtorController,
      controllerAs: 'InputValidate',

    };
  });
