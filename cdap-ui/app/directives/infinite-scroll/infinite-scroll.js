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

angular.module(PKG.name+'.commons')
.directive('infiniteScroll', function ($timeout) {
  return {
    restrict: 'A',
    link: function (scope, elem, attrs) {
      // wrapping in timeout to have content loaded first and setting the scroll to the top
      var timeout = $timeout(function() {
        elem.prop('scrollTop', 10);
        elem.bind('scroll',_.debounce(scrollListener, 1000));
      }, 100);

      function scrollListener () {
       if (elem.prop('scrollTop') + elem.prop('offsetHeight') >= elem.prop('scrollHeight')) {
         scope.$apply(attrs.infiniteScrollNext);
       } else if (elem.prop('scrollTop') === 0) {
         scope.$apply(attrs.infiniteScrollPrev);
       }
     }

      scope.$on('$destroy', function () {
        elem.unbind('scroll');
        $timeout.cancel(timeout);
      });
    }
  };

});
