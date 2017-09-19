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

angular.module(PKG.name + '.feature.datasets')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('datasets', {
        abstract: true,
        template: '<ui-view/>',
        url: '/datasets',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        parent: 'ns'
      })

      .state('datasets.detail', {
        url: '/:datasetId',
        abstract: true,
        resolve: {
          explorableDatasets: function explorableDatasets(myExploreApi, $stateParams, $q, $filter, myDatasetApi) {
            var params = {
              namespace: $stateParams.namespace
            };
            var defer = $q.defer(),
                filterFilter = $filter('filter');

            // Checking whether dataset is explorable
            $q.all([
              myDatasetApi.list(params).$promise, myExploreApi.list(params).$promise
            ])
              .then(
                function success(res) {
                  let datasetsSpec = res[0];
                  let exploreTables = res[1];
                  datasetsSpec = datasetsSpec
                    .filter(dSpec => dSpec.properties['explore.database.name'] || dSpec.properties['explore.table.name'])
                    .map(dSpec => {
                      return {
                        datasetName: dSpec.name,
                        database: dSpec.properties['explore.database.name'] || 'default',
                        table: dSpec.properties['explore.table.name'] || ''
                      };
                    });
                  let tables = exploreTables.map(tb => {
                    let tableIndex = tb.table.indexOf('_');
                    let dbIndex = tb.database.indexOf('_');
                    let matchingSpec = datasetsSpec.find(dspec => {
                      let isSameTable = dspec.table === (tableIndex !== -1 ? tb.table.slice(tableIndex) : tb.table);
                      let isSameDB = dspec.database === (dbIndex !== -1 ? tb.database.slice(dbIndex) : tb.table);
                      return isSameTable || isSameDB;
                    });
                    if (matchingSpec) {
                      let matchingSpecIndex = _.findIndex(datasetsSpec, matchingSpec);
                      datasetsSpec.splice(matchingSpecIndex, 1);
                      return {
                        table: matchingSpec.table || tb.table,
                        database: matchingSpec.database || tb.database
                      };
                    }
                    return tb;
                  });
                  if (datasetsSpec.length) {
                    tables = [...tables, ...datasetsSpec];
                  }
                  tables = [...tables || []];
                  var datasetId = $stateParams.datasetId;
                  datasetId = datasetId.replace(/[\.\-]/g, '_');

                  var match = filterFilter(tables, datasetId);

                  if (match.length === 0) {
                    defer.resolve(false);
                  } else {
                    defer.resolve(true);
                  }
                },
                function error() {
                  defer.resolve(false);
                }
              );
            return defer.promise;
          }
        },
        template: '<ui-view/>'
      })
        .state('datasets.detail.overview', {
          url: '/overview',
          templateUrl: '/old_assets/features/datasets/templates/detail.html',
          controller: 'DatasetsDetailController',
          controllerAs: 'DetailController',
          ncyBreadcrumb: {
            skip: true
          }
        })

          .state('datasets.detail.overview.status', {
            url: '/status',
            templateUrl: '/old_assets/features/datasets/templates/tabs/status.html',
            controller: 'DatasetDetailStatusController',
            controllerAs: 'StatusController',
            ncyBreadcrumb: {
              parent: 'data.list',
              label: '{{$state.params.datasetId}}'
            }
          })

          .state('datasets.detail.overview.explore', {
            url: '/explore',
            templateUrl: '/old_assets/features/datasets/templates/tabs/explore.html',
            controller: 'DatasetExploreController',
            controllerAs: 'ExploreController',
            ncyBreadcrumb: {
              label: 'Explore',
              parent: 'datasets.detail.overview.status'
            },
            resolve: {
              explorableDatasets: function(myExploreApi, $q, myDatasetApi, $stateParams) {
                var defer = $q.defer();
                var params = {
                  namespace: $stateParams.namespace
                };
                $q.all([myDatasetApi.list(params).$promise, myExploreApi.list(params).$promise])
                  .then(function (res) {
                    var exploreTables = res[1];
                    var datasetSpecs = res[0];
                    datasetSpecs = datasetSpecs
                    .filter(dSpec => dSpec.properties['explore.database.name'] || dSpec.properties['explore.table.name'])
                    .map(dSpec => {
                      return {
                        datasetName: dSpec.name,
                        database: dSpec.properties['explore.database.name'] || 'default',
                        table: dSpec.properties['explore.table.name'] || ''
                      };
                    });
            
                    let tables = exploreTables.map(tb => {
                      let tableIndex = tb.table.indexOf('_');
                      let dbIndex = tb.database.indexOf('_');
                      let matchingSpec = datasetSpecs.find(dspec => {
                        let isSameTable = (dspec.table || '').toLowerCase() === (tableIndex !== -1 ? tb.table.slice(tableIndex) : tb.table);
                        let isSameDB = (dspec.database || '').toLowerCase() === (dbIndex !== -1 ? tb.database.slice(dbIndex) : tb.database);
                        return isSameTable || isSameDB;
                      });
                      if (matchingSpec) {
                        let matchingSpecIndex = _.findIndex(datasetSpecs, matchingSpec);
                        datasetSpecs.splice(matchingSpecIndex, 1);
                        return {
                          table: matchingSpec.table || tb.table,
                          database: matchingSpec.database || tb.database,
                          type: matchingSpec.type || '',
                          datasetName: matchingSpec.datasetName
                        };
                      }
                      if (tableIndex === -1) {
                        tb.type = 'dataset';
                      } else {
                        var split = tb.table.split('_');
                        tb.type = split[0];
                      }
                      return tb;
                    });
            
            
                    if (datasetSpecs.length) {
                      tables = tables.concat(datasetSpecs);
                    }
            
                    return defer.resolve(tables);
                  });
                return defer.promise;
              }
            }
          })

          .state('datasets.detail.overview.programs', {
            url: '/programs',
            templateUrl: '/old_assets/features/datasets/templates/tabs/programs.html',
            ncyBreadcrumb: {
              label: 'Programs',
              parent: 'datasets.detail.overview.status'
            },
            controller: 'DatasetDetailProgramsController',
            controllerAs: 'ProgramsController'
          })

          .state('datasets.detail.overview.metadata', {
            url: '/metadata',
            templateUrl: '/old_assets/features/datasets/templates/tabs/metadata.html',
            ncyBreadcrumb: {
              label: 'Metadata',
              parent: 'datasets.detail.overview.status'
            },
            controller: 'DatasetMetadataController',
            controllerAs: 'MetadataController'
          });
  });
