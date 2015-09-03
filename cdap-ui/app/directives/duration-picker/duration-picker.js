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
  .directive('myDurationPicker', function () {
    return {
      restrict: 'E',
      require: 'ngModel',
      scope: {},
      templateUrl: 'duration-picker/duration-picker.html',

      link: function(scope, element, attrs, ngModel) {
        // Note: the 'timerange' field must be in a format accepted by the Metrics System
        scope.durationTypes = [
          {name: '1 Minute', startTime: 'now-1m'},
          {name: '1 Hour',   startTime: 'now-1h'},
          {name: '1 Day',    startTime: 'now-1d'},
        ];

        scope.durationType = scope.durationTypes[0];

        // NOTE: we can remove this if we don't want to show an icon next to Time Range selector (past X minutes)
        scope.label = attrs.label || 'Duration';

        ngModel.$render = function () {
          scope.timestamp = ngModel.$viewValue;
        };

        scope.$watch('durationType', function (newVal, oldVal, scope) {
          ngModel.$setViewValue(scope.durationType.startTime);
        });
      }
    };
  });
