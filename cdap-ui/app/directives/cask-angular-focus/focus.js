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
 * caskFocus
 *
 * add the cask-focus attribute to elements that you will want to trigger focus/select on
 *  then in the controller, use caskFocusManager to trigger the DOM event
 *
 * in template:
 * <input type="text" cask-focus="aNameForTheField" />
 *
 * in controller, inject caskFocusManager, then:
 * caskFocusManager.focus('aNameForTheField');
 */

angular.module(PKG.name+'.commons').directive('caskFocus',
function caskFocusDirective ($timeout, caskFocusManager) {
  return {

    restrict: 'A',

    link: function (scope, element, attrs) {

      attrs.$observe('caskFocus', function (newVal) {

        var cleanup = caskFocusManager.is.$watch(newVal, function (o) {
          if(o) {
            $timeout(function() {
              if(o.focus) {
                element[0].focus();
              }
              else if(o.select) {
                element[0].select();
              }
            });
          }
        });

        scope.$on('$destroy', function() {
          cleanup();
        });
      });

    }
  };
});
