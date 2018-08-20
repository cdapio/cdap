/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

/**
 * caskProgress
 *
 *  <cask-progress
 *      data-type="bar"
 *      data-add-cls="success striped"
 *      data-value="model.progress.stepscompleted"
 *      data-max="model.progress.stepstotal"
 *    ></cask-progress>
 */

angular.module(PKG.name+'.commons').directive('caskProgress',
function caskProgressDirective () {
  return {
    restrict: 'E',
    templateUrl: 'cask-angular-progress/bar.html',
    replace: true,
    scope: {
      addCls: '@',
      value: '=',
      max: '='
    },
    link: function(scope, element, attrs) {

      scope.$watch('value', function(newVal) {
        var max = parseInt(scope.max, 10) || 100;

        scope.percent = Math.floor((newVal / max) * 100);

        var cls = {
          'active': (newVal < max),
          'progress-bar': true
        };

        if(scope.addCls) {
          angular.forEach(scope.addCls.split(' '), function(add) {
            if(add) {
              switch (attrs.type) {
                case 'bar':
                /* falls through */
                default:
                  cls['progress-bar-'+add] = true;
                  break;
              }
            }
          });
        }


        scope.cls = cls;
      });
    }

  };
});
