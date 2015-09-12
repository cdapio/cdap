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

angular.module(PKG.name + '.feature.adapters')
  .controller('AdpaterDetailController', function($scope, rAdapterDetail, GLOBALS, MyAppDAGService, CanvasFactory, $state, myWorkFlowApi, myWorkersApi, myMapreduceApi, $timeout) {
    $scope.GLOBALS = GLOBALS;
    $scope.template = rAdapterDetail.template;
    $scope.description = rAdapterDetail.description;
    $scope.isScheduled = false;

    $scope.app = rAdapterDetail;

    $scope.canvasOperations = [
      { name: 'Start' },
      { name: 'Run Once'},
      { name: 'Delete' }
    ];

    $scope.tabs = [
      {
        title: 'Status',
        template: '/assets/features/adapters/templates/tabs/status.html'
      },
      {
        title: 'History',
        template: '/assets/features/adapters/templates/tabs/history.html'
      },
      {
        title: 'Log',
        template: '/assets/features/adapters/templates/tabs/log.html'
      },
      {
        title: 'Metrics',
        template: '/assets/features/adapters/templates/tabs/metrics.html'
      },
      {
        title: 'Configuration',
        template: '/assets/features/adapters/templates/tabs/configuration.html'
      },
      {
        title: 'Datasets',
        template: '/assets/features/adapters/templates/tabs/datasets.html'
      }
    ];

    $scope.programType = rAdapterDetail.artifact.name === GLOBALS.etlBatch ? 'WORKFLOWS' : 'WORKERS';

    var params = {
      namespace: $state.params.namespace,
      appId: rAdapterDetail.name,
      scope: $scope
    };

    var api;
    var logsApi;
    var logsParams = {
      namespace: $state.params.namespace,
      appId: rAdapterDetail.name,
      max: 50,
      scope: $scope
    };

    $scope.logs = [];

    if ($scope.programType === 'WORKFLOWS') {
      api = myWorkFlowApi;

      angular.forEach(rAdapterDetail.programs, function (program) {
        if (program.type === 'Workflow') {
          params.workflowId = program.id;
        } else if (program.type === 'Mapreduce') {
          logsParams.mapreduceId = program.id;
        }
      });
    } else {
      api = myWorkersApi;

      angular.forEach(rAdapterDetail.programs, function (program) {
        if (program.type === 'Workers') {
          params.workerId = program.id;
          logsParams.workerId = program.id;
        }
      });
    }

    $scope.runs = [];

    api.runs(params)
      .$promise
      .then(function (res) {
        $scope.runs = res;

        if ($scope.programType === 'WORKFLOWS') {
          logsApi = myMapreduceApi;

          $scope.loadingNext = true;
          logsApi.runs(logsParams)
            .$promise
            .then(function (runs) {
              logsParams.runId = runs[0].runid;

              logsApi.prevLogs(logsParams)
                .$promise
                .then(function (logs) {
                  $scope.logs = logs;
                  $scope.loadingNext = false;
                });
            });
        } else {
          logsApi = myWorkersApi;

          $scope.loadingNext = true;

          logsParams.runId = $scope.runs[0].runid;

          logsApi.prevLogs(logsParams)
            .$promise
            .then(function (logs) {
              $scope.logs = logs;
              $scope.loadingNext = false;
            });
        }
      });


    $scope.loadNextLogs = function () {
      if ($scope.loadingNext) {
        return;
      }

      $scope.loadingNext = true;
      logsParams.fromOffset = $scope.logs[$scope.logs.length-1].offset;

      myMapreduceApi.nextLogs(logsParams)
        .$promise
        .then(function (res) {
          $scope.logs = _.uniq($scope.logs.concat(res));
          $scope.loadingNext = false;
        });
    };

    $scope.loadPrevLogs = function () {
      if ($scope.loadingPrev) {
        return;
      }

      $scope.loadingPrev = true;
      logsParams.fromOffset = $scope.logs[0].offset;

      myMapreduceApi.prevLogs(logsParams)
        .$promise
        .then(function (res) {
          $scope.logs = _.uniq(res.concat($scope.logs));
          $scope.loadingPrev = false;

          $timeout(function() {
            document.getElementById(logsParams.fromOffset).scrollIntoView();
          });
        });
    };


    $scope.activeTab = $scope.tabs[0];

    $scope.selectTab = function(tab) {
      $scope.activeTab = tab;
    };
    $scope.nodes = [];

    function initializeDAG() {
      try{
        rAdapterDetail.config = JSON.parse(rAdapterDetail.configuration);
        $scope.config = rAdapterDetail.config;
      } catch(e) {
        console.log('ERROR in configuration from backend: ', e);
      }
      $scope.config = {
        name: $state.params.adapterId,
        template: rAdapterDetail.artifact.name,
        description: rAdapterDetail.description,
        config: {
          source: rAdapterDetail.config.source,
          sink: rAdapterDetail.config.sink,
          transforms: rAdapterDetail.config.transforms,
          instances: rAdapterDetail.instance,
          schedule: rAdapterDetail.config.schedule
        }
      };

      MyAppDAGService.metadata.name = rAdapterDetail.name;
      MyAppDAGService.metadata.description = rAdapterDetail.description;
      MyAppDAGService.metadata.template.type = rAdapterDetail.artifact.name;
      if (rAdapterDetail.artifact.name === GLOBALS.etlBatch) {
        MyAppDAGService.metadata.template.schedule = rAdapterDetail.config.schedule;
      } else if (rAdapterDetail.artifact.name === GLOBALS.etlRealtime) {
        MyAppDAGService.metadata.template.instances = rAdapterDetail.config.instances;
      }

      $scope.source = rAdapterDetail.config.source;
      $scope.sink = rAdapterDetail.config.sink;
      $scope.transforms = rAdapterDetail.config.transforms;
      $scope.nodes = CanvasFactory.getNodes(rAdapterDetail.config);
      $scope.nodes.forEach(function(node) {
        MyAppDAGService.addNodes(node, node.type);
      });

      MyAppDAGService.connections = CanvasFactory.getConnectionsBasedOnNodes($scope.nodes);
    }

    initializeDAG();

    $scope.datasets = [];
    $scope.datasets = $scope.datasets.concat(
      rAdapterDetail.datasets.map(function (dataset) {
        dataset.type = 'Dataset';
        return dataset;
      }),
      rAdapterDetail.streams.map(function (stream) {
        stream.type = 'Stream';
        return stream;
      }));
  });
