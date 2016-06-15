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

  .directive('tooltipToggle', function ($timeout) {
    return {
      restrict: 'A',
      link: function (scope, element, attrs) {
        attrs.tooltipTrigger = 'customShow';

        var toggleTimeout = null;

        scope.$watch(attrs.tooltipToggle, function (newVal) {
          $timeout.cancel(toggleTimeout);
          toggleTimeout = $timeout(function () {
            if (newVal) {
              element.triggerHandler('customShow');
            } else {
              element.triggerHandler('customHide');
            }
          });
        });

        scope.$on('$destroy', function () {
          $timeout.cancel(toggleTimeout);
        });
      }
    };
  })

  .directive('tooltipEllipsis', function () {
    return {
      restrict: 'A',
      scope: {
        ellipsis: '=',
        offsetValue: '='
      },
      link: function (scope, element) {
        function isEllipsisActive(e) {
          return (e[0].offsetWidth > e[0].parentElement.offsetWidth - scope.offsetValue);
        }
        // FIXME: Find more efficient approach
        scope.$watch(function () {
          return element[0].parentElement.offsetWidth;
        }, function () {
          if (isEllipsisActive(element)) {
            scope.ellipsis = true;
          } else {
            scope.ellipsis = false;
          }
        });

      }
    };
  });
