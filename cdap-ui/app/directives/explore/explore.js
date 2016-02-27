/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
  .directive('myExplore', function () {

    return {
      restrict: 'E',
      scope: {
        type: '=',
        name: '='
      },
      templateUrl: 'explore/explore.html',
      controller: myExploreCtrl,
      controllerAs: 'MyExploreCtrl'
    };


    function myExploreCtrl ($scope, myExploreApi, $http, $state, $uibModal, myCdapUrl, $timeout, myAlert, MY_CONFIG) {
        var vm = this;

        vm.queries = [];
        vm.currentPage = 1;
        var params = {
          namespace: $state.params.namespace,
          scope: $scope
        };

        $scope.$watch('name', function() {
          vm.query = 'SELECT * FROM ' + $scope.type + '_' + $scope.name + ' LIMIT 5';
        });

        vm.execute = function() {
          myExploreApi.postQuery(params, { query: vm.query }, vm.getQueries);
          vm.currentPage = 1;
        };

        vm.getQueries = function() {

          myExploreApi.getQueries(params)
            .$promise
            .then(function (queries) {
              vm.queries = queries;

              // Polling for status
              angular.forEach(vm.queries, function(q) {
                q.isOpen = false;
                if (q.status !== 'FINISHED') {

                  var statusParams = {
                    queryhandle: q.query_handle,
                    scope: $scope
                  };

                  myExploreApi.pollQueryStatus(statusParams)
                    .$promise
                    .then(function (res) {
                      q.status = res.status;

                      if (res.status === 'FINISHED') {
                        myExploreApi.stopPollQueryStatus(statusParams);
                      }
                    });

                }
              });

              $timeout(function () {
                vm.previous = vm.queries.map(function (q) { return q.query_handle; });
              }, 1000);

            });
        };

        vm.getQueries();

        vm.preview = function (query) {
          $uibModal.open({
            templateUrl: 'explore/preview-modal.html',
            size: 'lg',
            resolve: {
              query: function () { return query; }
            },
            controller: ['$scope', 'myExploreApi', 'query', function ($scope, myExploreApi, query) {
              var params = {
                queryhandle: query.query_handle,
                scope: $scope
              };

              myExploreApi.getQuerySchema(params)
                .$promise
                .then(function (res) {
                  angular.forEach(res, function(column) {
                    // check for '.' in the name, before splitting on it, because in the case that specific columns are
                    // queried, the column names in the schema are not prefixed by the dataset name
                    if (column.name.indexOf('.') != -1) {
                      column.name = column.name.split('.')[1];
                    }
                  });

                  $scope.schema = res;
                });

              myExploreApi.getQueryPreview(params, {},
                function (res) {
                  $scope.rows = res;
                });

            }]
          });
        };


        vm.download = function(query) {
          query.downloading = true;
          query.is_active = false; // this will prevent user from previewing after download

          // Cannot use $resource: http://stackoverflow.com/questions/24876593/resource-query-return-split-strings-array-of-char-instead-of-a-string

          // The files are being stored in the node proxy

          $http.post('/downloadQuery', {
            'backendUrl': myCdapUrl.constructUrl({_cdapPath: '/data/explore/queries/' + query.query_handle + '/download'}),
            'queryHandle': query.query_handle
          })
            .success(function(res) {

              var url = (MY_CONFIG.sslEnabled? 'https://': 'http://') + window.location.host + res;

              var element = angular.element('<a/>');
              element.attr({
                href: url,
                target: '_self'
              })[0].click();

              query.downloading = false;
            })
            .error(function(error) {
              console.info('Error downloading query');
              myAlert({
                title: 'Error',
                content: error
              });

              query.downloading = false;
            });
        };

        vm.clone = function (query) {
          vm.query = query.statement;
        };

      }


  });
