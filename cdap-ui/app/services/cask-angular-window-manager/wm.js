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

angular.module(PKG.name+'.services')

  .constant('CASK_WM_EVENT', {
    resize: 'cask-wm-resize',
    blur: 'cask-wm-blur',
    focus: 'cask-wm-focus'
  })

  .provider('caskWindowManager', function (CASK_WM_EVENT) {

    this.resizeDebounceMs = 500;

    this.pageViz = {
      hidden: 'visibilitychange',
      mozHidden: 'mozvisibilitychange',
      msHidden: 'msvisibilitychange',
      webkitHidden: 'webkitvisibilitychange'
    };

    this.$get = function ($rootScope, $window, $document, $log, $timeout) {

      // resize inspired by https://github.com/danmasta/ngResize
      var resizeDebounceMs = this.resizeDebounceMs,
          resizePromise = null;

      angular.element($window).on('resize', function () {
        if(resizePromise) {
          $timeout.cancel(resizePromise);
        }
        resizePromise = $timeout(function () {
          $log.log('[caskWindowManager]', 'resize');
          $rootScope.$broadcast(CASK_WM_EVENT.resize, {
            width: $window.innerWidth,
            height: $window.innerHeight
          });
        }, resizeDebounceMs, false);
      });



      // pageviz inspired by https://github.com/mz026/angular_page_visibility
      var mkOnVizChange = function (q) {
        return function (e) {
          $log.log('[caskWindowManager]', e);
          $rootScope.$broadcast(
            CASK_WM_EVENT[ $document.prop(q) ? 'blur' : 'focus' ]
          );
        };
      };

      var vizImp = Object.keys(this.pageViz);
      for (var i = 0; i < vizImp.length; i++) { // iterate through implementations
        var p = vizImp[i];
        if (typeof ($document.prop(p)) !== 'undefined') {
          $log.info('[caskWindowManager] page visibility API available!');
          $document.on(this.pageViz[p], mkOnVizChange(p));
          break;
        }
      }

      return {
        event: CASK_WM_EVENT
      };

    };

  })


  /*
  * caskOnWm Directive
  *
  * usage: cask-on-wm="{resize:expression()}"
  * event data is available as $event
  */
  .directive('caskOnWm', function ($parse, $timeout, caskWindowManager) {
    return {
      compile: function ($element, attr) {
        var obj = $parse(attr.caskOnWm);
        return function (scope) {
          angular.forEach(obj, function (fn, key) {
            var eName = caskWindowManager.event[key];
            if(eName) {
              scope.$on(eName, function (event, data) {
                $timeout(function () {
                  scope.$apply(function () {
                    fn(scope, { $event: data });
                  });
                });
              });
            }
          });
        };
      }
    };
  });
