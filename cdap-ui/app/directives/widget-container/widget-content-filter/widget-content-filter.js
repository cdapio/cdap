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
  .directive('myContentFilter', function () {
    return {
      restrict: 'E',
      templateUrl: 'widget-container/widget-content-filter/widget-content-filter.html',
      // bindToController: true,
      scope: {
        disabled: '=',
        model: '=ngModel',
        config: '='
      },
      controller: function ContentFilterController($scope, myHelpers, EventPipe) {
        'ngInject';

        function init() {
          try {
            // Initialize select element options.
            $scope.fieldOptions = [];
            $scope.fieldOptions = $scope.config['widget-attributes'].options;
            // Initial value of select element.
            let previousValue = $scope.model;
            if(!previousValue) {
              previousValue = '';
            }
            $scope.filterValue = previousValue;
          } catch (e) {
            console.log('Error', e);
          }
        }
        init();

        let filterValueListener = $scope.$watch('filterValue', () => {
          $scope.model = $scope.filterValue;

          if($scope.filterValue !== '') {
            EventPipe.emit('content-filter.changed', $scope.config['name'], $scope.filterValue);
          }
        }, true);

        $scope.$on('$destroy', function () {
          // Event to notify listeners that widgets is destroyed
          EventPipe.emit('content-filter.destroyed', $scope.config['name']);
          filterValueListener();
        });
      }
    };
  });