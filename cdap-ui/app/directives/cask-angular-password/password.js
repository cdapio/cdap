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
 * caskPassword
 *
 * implements "click2show" behavior
 *
 * <cask-password data-value="password"></cask-password>
 */

angular.module(PKG.name+'.commons').directive('caskPassword',
function caskPasswordDirective (caskFocusManager) {
  return {
    restrict: 'E',
    templateUrl: 'cask-angular-password/click2show.html',
    replace: true,
    scope: {
      value: '='
    },
    link: function(scope) {

      scope.uid = ['caskPassword', Date.now(), Math.random().toString().substr(2)].join('_');

      scope.doToggle = function() {
        var show = !scope.show;
        scope.show = show;
        if (show) {
          caskFocusManager.select(scope.uid);
        }
      };

    }

  };
});
