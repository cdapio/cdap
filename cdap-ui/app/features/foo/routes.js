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

angular.module(PKG.name+'.feature.foo')
  .config(function ($stateProvider, MYAUTH_ROLE) {


    /**
     * State Configurations
     */
    $stateProvider

      .state('foo', {
        url: '/foo',
        templateUrl: '/assets/features/foo/foo.html'
      })
      .state('test-plumb', {
        url: '/foo/plumb',
        controller: 'PlumbController',
        controllerAs: 'PlumbController',
        templateUrl: '/assets/features/foo/plumb/plumb.html'
      })
      .state('test-edwin', {
        url: '/test/edwin',
        templateUrl: '/assets/features/foo/edwin.html',
        controller: function ($scope, $timeout) {
          $scope.settings = {
            size: {
              height: 100,
              width: 280
            },
            chartMetadata: {
              showx: true,
              showy: true,
              legend: {
                show: true,
                position: 'inset'
              }
            },
            color: {
              pattern: ['red', '#f4b400']
            },
            isLive: true,
            interval: 60*1000,
            aggregate: 5
          };

          $scope.data = {
            columns: [],
            keys: 'x',
            x: 'x',
            type: 'line'
          };

          var latestTime = 1435686300;

          function generateData() {
            var columns = [];

            columns.push(['x']);

            for (var i = 0; i < 5; i++) {
              columns[0].push(latestTime + 300);
              latestTime += 300;
            }

            columns.push(['data1']);
            columns.push(['data2']);
            for (var i = 0; i<5; i++) {
              columns[1].push(Math.floor(Math.random() * 101));
              columns[2].push(Math.floor(Math.random() * 101));
            }

            $scope.data.columns = columns;

            $timeout(function() {
              generateData();
            }, 3000);
          }

          generateData();

        }
      })

      .state('rule-driver', {
        url: '/ruledriver',
        templateUrl: '/assets/features/foo/hack1/hack1.html',
        controller: 'RuleDriverController',
        controllerAs: 'RuleDriverController'
      })
      .state('test-settings', {
        url: '/test/settings',
        templateUrl: '/assets/features/foo/settings.html',
        controller: 'FooPlaygroundController'
      })
      .state('validators-test', {
        url: '/validator',
        templateUrl: 'assets/features/foo/validator.html',
        controllerAs: 'ValidatorCtrl',
        controller: function () {
          this.inputFields = [
            {
              name: 'field1',
              type: 'string'
            },
            {
              name: 'field2',
              type: 'number'
            },
            {
              name: 'field3',
              type: 'boolean'
            }
          ];

          this.model = {
            errorDatasetName: '',
            properties: ''
          };

        }
      });

  });
