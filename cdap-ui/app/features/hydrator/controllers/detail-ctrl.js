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

angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorDetailController', function($scope, rPipelineDetail, GLOBALS, MyAppDAGService, CanvasFactory, $state, myWorkFlowApi, myWorkersApi, myAppsApi, HydratorDetail, $timeout, MyNodeConfigService, $alert) {
    $scope.GLOBALS = GLOBALS;
    $scope.template = rPipelineDetail.template;
    $scope.description = rPipelineDetail.description;
    $scope.app = rPipelineDetail;
    $scope.runOnceLoading = false;
    MyAppDAGService.registerEditPropertiesCallback(viewProperties.bind(this));

    HydratorDetail.initialize(rPipelineDetail, $state);

    $scope.tabs = [
      {
        title: 'Status',
        template: '/assets/features/hydrator/templates/tabs/status.html'
      },
      {
        title: 'History',
        template: '/assets/features/hydrator/templates/tabs/history.html'
      },
      {
        title: 'Log',
        template: '/assets/features/hydrator/templates/tabs/log.html'
      },
      {
        title: 'Metrics',
        template: '/assets/features/hydrator/templates/tabs/metrics.html'
      },
      {
        title: 'Configuration',
        template: '/assets/features/hydrator/templates/tabs/configuration.html'
      },
      {
        title: 'Datasets',
        template: '/assets/features/hydrator/templates/tabs/datasets.html'
      },
      {
        title: 'Node Configuration',
        template: '/assets/features/hydrator/templates/tabs/node-configuration.html'
      }
    ];

    function checkCron(cron) {
      var pattern = /^[0-9\*\s]*$/g;
      var parse = cron.split('');
      for (var i = 0; i < parse.length; i++) {
        if (!parse[i].match(pattern)) {
          return false;
        }
      }
      return true;
    }

    if (HydratorDetail.programType === 'WORKFLOWS') {
      $scope.schedule = HydratorDetail.schedule;
      $scope.isBasic = checkCron($scope.schedule);
      $scope.tabs.push({
        title: 'Schedule',
        template: '/assets/features/hydrator/templates/tabs/schedule.html'
      });
    } else {
      $scope.instances = HydratorDetail.instances;
      $scope.tabs.push({
        title: 'Instance',
        template: '/assets/features/hydrator/templates/tabs/instance.html'
      });
    }

    $scope.activeTab = $scope.tabs[0];
    $scope.selectTab = function(tab) {
      $scope.activeTab = tab;
    };

    function viewProperties(plugin) {
      $scope.selectTab($scope.tabs[6]);
      // Giving 100ms to load the template and then set the plugin
      // For this service to work the controller has to register a callback
      // with the service. The callback will not be called if plugin assignment happens
      // before controller initialization. Hence the 100ms delay.
      $timeout(function() {
        MyNodeConfigService.setPlugin(plugin);
      }, 100);
    }
    var params = {
      namespace: $state.params.namespace,
      appId: rPipelineDetail.name,
      scope: $scope
    };

    if (HydratorDetail.programType === 'WORKFLOWS') {
      angular.forEach(rPipelineDetail.programs, function (program) {
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
              appId: rPipelineDetail.name,
              scheduleId: 'etlWorkflow',
              scope: $scope
            })
              .$promise
              .then(function (schedule) {
                $scope.appStatus = schedule.status;
                $scope.scheduleStatus = schedule.status;
              });
          }
        });

    } else {

      angular.forEach(rPipelineDetail.programs, function (program) {
        if (program.type === 'Worker') {
          params.workerId = program.id;
        }
      });

      myWorkersApi.pollStatus(params)
        .$promise
        .then(function (res) {
          $scope.appStatus = res.status === 'RUNNING' ? 'RUNNING' : 'SUSPENDED';
        });
    }


    $scope.do = function (action) {
      var scheduleParams = {
        namespace: $state.params.namespace,
        appId: rPipelineDetail.name,
        scheduleId: 'etlWorkflow',
        scope: $scope
      };

      switch (action) {
        case 'Start':
          $scope.appStatus = 'STARTING';

          if (HydratorDetail.programType === 'WORKFLOWS') {
            myWorkFlowApi.scheduleResume(scheduleParams, {})
              .$promise
              .then(function () {
                $scope.appStatus = 'SCHEDULED';
                $scope.scheduleStatus = 'SCHEDULED';
              });
          } else {
            myWorkersApi.doAction(angular.extend(params, { action: 'start' }), {})
              .$promise
              .then(function () {
                $scope.appStatus = 'RUNNING';
              }, function (err) {
                $alert({
                  type: 'danger',
                  content: err.data
                });
                $scope.appStatus = 'FAILED';
              });
          }

          break;

        case 'Stop':
          $scope.appStatus = 'STOPPING';
          if (HydratorDetail.programType === 'WORKFLOWS') {
            myWorkFlowApi.scheduleSuspend(scheduleParams, {})
              .$promise
              .then(function () {
                $scope.appStatus = 'SUSPENDED';
                $scope.scheduleStatus = 'SUSPENDED';
              });
          } else {
            myWorkersApi.doAction(angular.extend(params, { action: 'stop' }), {})
              .$promise
              .then(function () {
                $scope.appStatus = 'SUSPENDED';
              });
          }

          break;
        case 'Run Once':
          $scope.runOnceLoading = true;
          myWorkFlowApi.doAction(angular.extend(params, { action: 'start' }), {})
            .$promise
            .then(function () {
              $scope.appStatus = 'RUNNING';
              $scope.runOnceLoading = false;
            }, function error (err) {
              $alert({
                type: 'danger',
                content: err.data
              });
              $scope.runOnceLoading = false;
            });

          break;
        case 'Delete':
          var deleteParams = {
            namespace: $state.params.namespace,
            appId: rPipelineDetail.name,
            scope: $scope
          };

          myAppsApi.delete(deleteParams)
            .$promise
            .then(function () {
              console.log('Successfully Deleted Hydrator App');
              $state.go('hydrator.list');
            });

          break;
      }
    };


    $scope.nodes = [];

    function initializeDAG() {
      try{
        rPipelineDetail.config = JSON.parse(rPipelineDetail.configuration);
      } catch(e) {
        console.log('ERROR in configuration from backend: ', e);
        return;
      }
      $scope.config = {
        name: $state.params.pipelineId,
        artifact: rPipelineDetail.artifact,
        template: rPipelineDetail.artifact.name,
        description: rPipelineDetail.description,
        config: {
          source: rPipelineDetail.config.source,
          sinks: rPipelineDetail.config.sinks,
          transforms: rPipelineDetail.config.transforms,
          instances: rPipelineDetail.instance,
          schedule: rPipelineDetail.config.schedule
        }
      };

      MyAppDAGService.metadata.name = rPipelineDetail.name;
      MyAppDAGService.metadata.description = rPipelineDetail.description;
      MyAppDAGService.metadata.template.type = rPipelineDetail.artifact.name;
      if (rPipelineDetail.artifact.name === GLOBALS.etlBatch) {
        MyAppDAGService.metadata.template.schedule = rPipelineDetail.config.schedule;
      } else if (rPipelineDetail.artifact.name === GLOBALS.etlRealtime) {
        MyAppDAGService.metadata.template.instances = rPipelineDetail.config.instances;
      }

      $scope.nodes = CanvasFactory.getNodes(rPipelineDetail.config, rPipelineDetail.artifact.name);
      $scope.nodes.forEach(function(node) {
        MyAppDAGService.addNodes(node, node.type);
      });

      MyAppDAGService.connections = CanvasFactory.getConnectionsBasedOnNodes($scope.nodes, rPipelineDetail.artifact.name);
    }

    initializeDAG();

    $scope.datasets = [];
    $scope.datasets = $scope.datasets.concat(
      rPipelineDetail.datasets.map(function (dataset) {
        dataset.type = 'Dataset';
        return dataset;
      }),
      rPipelineDetail.streams.map(function (stream) {
        stream.type = 'Stream';
        return stream;
      }));

  });
