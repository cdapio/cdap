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
  .controller('AdpaterDetailController', function($scope, rAdapterDetail, GLOBALS, MyAppDAGService, CanvasFactory, $state, myWorkFlowApi, myWorkersApi, myAppsApi, AdapterDetail) {
    $scope.GLOBALS = GLOBALS;
    $scope.template = rAdapterDetail.template;
    $scope.description = rAdapterDetail.description;
    $scope.app = rAdapterDetail;

    AdapterDetail.initialize(rAdapterDetail, $state);

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

    $scope.activeTab = $scope.tabs[0];
    $scope.selectTab = function(tab) {
      $scope.activeTab = tab;
    };


    var params = {
      namespace: $state.params.namespace,
      appId: rAdapterDetail.name,
      scope: $scope
    };

    if (AdapterDetail.programType === 'WORKFLOWS') {
      angular.forEach(rAdapterDetail.programs, function (program) {
        if (program.type === 'Workflow') {
          params.workflowId = program.id;
        }
      });

      myWorkFlowApi.pollStatus(params)
        .$promise
        .then(function (res) {
          if (res.status === 'RUNNING') {
            $scope.appStatus = res.status;
          } else {
            myWorkFlowApi.getScheduleStatus({
              namespace: $state.params.namespace,
              appId: rAdapterDetail.name,
              scheduleId: 'etlWorkflow',
              scope: $scope
            })
              .$promise
              .then(function (schedule) {
                $scope.appStatus = schedule.status;
              });
          }
        });

    } else {

      angular.forEach(rAdapterDetail.programs, function (program) {
        if (program.type === 'Worker') {
          params.workerId = program.id;
        }
      });

      myWorkersApi.pollStatus(params)
        .$promise
        .then(function (res) {
          $scope.appStatus = res.status;
        });
    }


    $scope.do = function (action) {
      var scheduleParams = {
        namespace: $state.params.namespace,
        appId: rAdapterDetail.name,
        scheduleId: 'etlWorkflow',
        scope: $scope
      };

      switch (action) {
        case 'Start':
          $scope.appStatus = 'Starting';

          if (AdapterDetail.programType === 'WORKFLOWS') {
            myWorkFlowApi.scheduleResume(scheduleParams, {})
              .$promise
              .then(function () {
                $scope.appStatus = 'SCHEDULED';
              });
          } else {
            myWorkersApi.doAction(angular.extend(params, { action: 'start' }), {})
              .$promise
              .then(function () {
                $scope.appStatus = 'RUNNING';
              }, function () {
                $scope.appStatus = 'FAILED';
              });
          }

          break;

        case 'Stop':
          $scope.appStatus = 'Stopping';
          if (AdapterDetail.programType === 'WORKFLOWS') {
            myWorkFlowApi.scheduleSuspend(scheduleParams, {})
              .$promise
              .then(function () {
                $scope.appStatus = 'SUSPENDED';
              });
          } else {
            myWorkersApi.doAction(angular.extend(params, { action: 'stop' }), {})
              .$promise
              .then(function () {
                $scope.appStatus = 'STOPPED';
              });
          }

          break;
        case 'Run Once':
          myWorkFlowApi.doAction(angular.extend(params, { action: 'start' }), {});

          break;
        case 'Delete':
          var deleteParams = {
            namespace: $state.params.namespace,
            appId: rAdapterDetail.name,
            scope: $scope
          };

          myAppsApi.delete(deleteParams)
            .$promise
            .then(function () {
              console.log('Successfully Deleted Hydrator App');
              $state.go('adapters.list');
            });

          break;
      }
    };


    $scope.nodes = [];

    function initializeDAG() {
      try{
        rAdapterDetail.config = JSON.parse(rAdapterDetail.configuration);
      } catch(e) {
        console.log('ERROR in configuration from backend: ', e);
        return;
      }
      $scope.config = {
        name: $state.params.adapterId,
        artifact: rAdapterDetail.artifact,
        template: rAdapterDetail.artifact.name,
        description: rAdapterDetail.description,
        config: {
          source: rAdapterDetail.config.source,
          sinks: rAdapterDetail.config.sinks,
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

      $scope.nodes = CanvasFactory.getNodes(rAdapterDetail.config, rAdapterDetail.artifact.name);
      $scope.nodes.forEach(function(node) {
        MyAppDAGService.addNodes(node, node.type);
      });

      MyAppDAGService.connections = CanvasFactory.getConnectionsBasedOnNodes($scope.nodes, rAdapterDetail.artifact.name);
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
