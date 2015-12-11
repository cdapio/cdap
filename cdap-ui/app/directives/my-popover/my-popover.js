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
  .directive('myPopover', function($compile, $popover, $timeout) {
    return {
      restrict: 'A',
      scope: {
        template: '=',
        contentData:'=',
        title: '@',
        placement: '@',
        popoverContext: '='
      },
      link: function(scope, element) {
        scope.isOpen = false;
        var popoverElement;
        var delayOpenTimer;
        var delayCloseTimer;
        var mypopover;

        var targetElement = angular.element(element);
        targetElement.removeAttr('my-popover');
        targetElement.removeAttr('data-template');
        targetElement.removeAttr('data-placement');
        targetElement.removeAttr('data-title');

        function cancelTimers() {
          if (delayOpenTimer) {
            $timeout.cancel(delayOpenTimer);
          }
          if (delayCloseTimer) {
            $timeout.cancel(delayCloseTimer);
          }
        }
        function delayClose(delay) {
          cancelTimers();
          delayCloseTimer = $timeout(function() {
            mypopover.hide();
            destroyPopover();
          }, delay);
          return delayCloseTimer;
        }
        function destroyPopover() {
          if (mypopover) {
            mypopover.destroy();
            mypopover = null;
          }
        }
        function createPopover() {
          mypopover = $popover(targetElement, {
            title: scope.title,
            contentTemplate: scope.template,
            show: false,
            placement: scope.placement || 'right',
            trigger: 'manual',
            container: 'body',
            customClass: 'my-cdap-popover'
          });
          if (scope.contentData) {
            mypopover.$scope.contentData = scope.contentData;
          }
          if (scope.popoverContext) {
            mypopover.$scope.popoverContext = scope.popoverContext;
          }
          mypopover.$scope.delayClose = delayClose;
          return mypopover.$promise;
        }
        function initPopover() {
          targetElement
            .on('mouseenter', function() {
              if (!mypopover) {
                createPopover().then(showPopover);
              } else {
                showPopover();
              }
            })
            .on('mouseleave', delayClose.bind(null, 100));
        }
        function showPopover() {
          cancelTimers();
          mypopover.show();
          popoverElement = mypopover.$element;
          popoverElement.on('mouseenter', cancelTimers);
          popoverElement.on('mouseleave', delayClose.bind(null, 100));
        }
        initPopover();
      }
    };
  });
