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
  .config(function($datepickerProvider, $timepickerProvider) {
    angular.extend($datepickerProvider.defaults, {
      iconLeft: 'fa fa-chevron-left',
      iconRight: 'fa fa-chevron-right'
    });
    angular.extend($timepickerProvider.defaults, {
      iconUp: 'fa fa-chevron-up',
      iconDown: 'fa fa-chevron-down'
    });
  })
  .directive('myTimestampPicker', function () {
    return {
      restrict: 'E',
      require: 'ngModel',
      scope: {},
      templateUrl: 'timestamp-picker/datetime.html',

      link: function(scope, element, attrs, ngModel) {

        scope.label = attrs.label || 'Timestamp';

        ngModel.$render = function () {
          scope.timestamp = ngModel.$viewValue;
        };

        scope.$watch('timestamp', function (newVal) {
          ngModel.$setViewValue(newVal);
        });
      }
    };
  });
