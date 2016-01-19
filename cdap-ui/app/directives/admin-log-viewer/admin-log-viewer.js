/*
 * Copyright © 2016 Cask Data, Inc.
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
  .directive('myAdminLogViewer', function ($filter, $timeout, $state, $location) {

    var capitalize = $filter('caskCapitalizeFilter'),
        filterFilter = $filter('filter');

    return {
      restrict: 'E',

      scope: {
        model: '=',
        next: '&',
        prev: '&',
        loadingNext: '=',
        loadingPrev: '='
      },

      templateUrl: 'admin-log-viewer/admin-log-viewer.html',

      controller: function ($scope) {

        $scope.filters = 'all,info,warn,error,debug,other'.split(',')
          .map(function (key) {
            var p;
            switch(key) {
              case 'all':
                p = function() { return true; };
                break;
              case 'other':
                p = function(line) { return !(/- (INFO|WARN|ERROR|DEBUG)/).test(line.log); };
                break;
              default:
                p = function(line) { return (new RegExp('- '+key.toUpperCase())).test(line.log); };
            }
            return {
              key: key,
              label: capitalize(key),
              entries: [],
              predicate: p
            };
          });

        $scope.$watch('model', function (newVal) {
          angular.forEach($scope.filters, function (one) {
            one.entries = filterFilter(newVal, one.predicate);
          });
        });

      },

      link: function (scope, element) {

        var termEl = angular.element(element[0].querySelector('.terminal')),
            QPARAM = 'filter';

        scope.setFilter = function (k) {
          var f = filterFilter(scope.filters, {key:k});
          scope.activeFilter = f.length ? f[0] : scope.filters[0];

          $timeout(function(){
            termEl.prop('scrollTop', termEl.prop('scrollHeight'));

            if(false === $state.current.reloadOnSearch) {
              var params = {};
              params[QPARAM] = scope.activeFilter.key;
              $location.search(params);
            }
          });

        };

        scope.setFilter($state.params[QPARAM]);
      }
    };
  });
