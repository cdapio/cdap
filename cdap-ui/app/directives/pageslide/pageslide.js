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

angular.module(PKG.name+ '.commons')
  .directive('myPageslide', function ($timeout) {
    return {
      restrict: 'E',
      transclude: false,
      scope: {
        open: '=',
        width: '@'
      },
      link: function pageslide(scope, elem) {
        scope.width = scope.width || '600';

        var slide = elem[0];

        slide.style.position = 'absolute';
        slide.style.overflowX = 'hidden';
        slide.style.width = '0px';
        slide.style.right = '0px';

        slide.style.bottom = '58px'; // height of footer
        slide.style.backgroundColor = '#F6F6F6';
        slide.style.padding = '0';
        slide.style.zIndex = 1000;
        slide.style.transitionDuration = '0.5s';
        slide.style.webkitTransitionDuration = '0.5s';
        slide.style.transitionProperty = 'width, height';
        slide.style.boxShadow = '-3px 0px 5px 0px rgba(0,0,0,0.25)';

        // The calculation of the offset top needs to happen after the template is rendered
        $timeout(function () {
          var anchor = angular.element(document.getElementById('sidepanel-anchor'));
          var offset = anchor[0].offsetTop + 2;
          slide.style.top = offset + 'px';
        });


        function open(slider) {
          slider.style.width = scope.width + 'px';
          slide.style.backgroundColor = '#F6F6F6';
          angular.element(elem[0].childNodes[1])[0].style.height = elem[0].scrollHeight;
        }

        function close(slider) {
          slider.style.width = '0px';
          scope.open = false;
        }

        scope.$watch('open', function (value) {
          if (!!value) {
            open(slide);
          } else {
            close(slide);
          }
        });

        scope.$on('$destroy', function () {
          close(slide);
        });

        scope.$on('$locationChangeStart', function () {
          close(slide);
        });

        scope.$on('$stateChangeStart', function () {
          close(slide);
        });

      }
    };
  });


