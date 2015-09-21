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

 /*
  For this directive to work we need to know the height of parent to calculate height of the current
    container which using this directive. For instance something like this,

    <div class="parent" parent-height>
      <div class="child" my-calculate-height>

      </div>
    </div>
  FIXME: This is not super intutive for now but this will fixed in release 3.3 of cdap.
 */

angular.module(PKG.name + '.commons')
  .directive('myCalculateHeight', function () {
    return {
      restrict: 'A',
      scope: {
        substractHeight: '='
      },
      link: function (scope, elem) {
        var element = elem[0],
            container = angular.element(document.querySelector('[parent-height]'));

        scope.$watch(function () {
          return container[0].offsetHeight;
        }, function () {
          var containerHeight = container[0].offsetHeight,
              height = containerHeight - scope.substractHeight;
          element.style.height = height + 'px';
        });
      }
    };
  });
