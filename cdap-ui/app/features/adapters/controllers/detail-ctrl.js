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
  .controller('AdpaterDetailController', function($scope, rAdapterDetail, GLOBALS, MyAppDAGService, CanvasFactory, $state, myWorkFlowApi, myWorkersApi, myMapreduceApi, $timeout, MyDataSource, MyMetricsQueryHelper, $filter, myAppsApi) {
    $scope.GLOBALS = GLOBALS;
    $scope.template = rAdapterDetail.template;
    $scope.description = rAdapterDetail.description;
    $scope.isScheduled = false;

    var source = [],
        transforms = [],
        sinks = [];

    $scope.app = rAdapterDetail;

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
      api = myWorkersApi;

      angular.forEach(rAdapterDetail.programs, function (program) {
        if (program.type === 'Workers') {
          params.workerId = program.id;
          logsParams.workerId = program.id;
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
          if ($scope.programType === 'WORKFLOWS') {
            myWorkFlowApi.scheduleResume(scheduleParams, {})
              .$promise
              .then(function () {
                $scope.appStatus = 'SCHEDULED';
              });
          } else {
            myWorkersApi.doAction(angular.extend(params, { action: 'start' }, {}))
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
          if ($scope.programType === 'WORKFLOWS') {
            myWorkFlowApi.scheduleSuspend(scheduleParams, {})
              .$promise
              .then(function () {
                $scope.appStatus = 'SUSPENDED';
              });
          } else {
            myWorkersApi.doAction(angular.extend(params, { action: 'stop' }, {}))
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



    $scope.runs = [];

    api.runs(params)
      .$promise
      .then(function (res) {
        $scope.runs = res;

        if ($scope.runs.length === 0) { return; }

        if ($scope.programType === 'WORKFLOWS') {

          api.getStatistics(params)
            .$promise
            .then(function (stats) {
              $scope.stats = {
                numRuns: stats.runs,
                avgRunTime: stats.avgRunTime,
                latestRun: $scope.runs[0]
              };
            });


          logsApi = myMapreduceApi;

          $scope.loadingNext = true;
          logsApi.runs(logsParams)
            .$promise
            .then(function (runs) {
              if (runs.length === 0) { return; }

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

        source = rAdapterDetail.config.source.name;
        transforms = rAdapterDetail.config.transforms.map(function (n) { return n.name; });
        sinks = rAdapterDetail.config.sinks.map(function (n) { return n.name; });
      } catch(e) {
        console.log('ERROR in configuration from backend: ', e);
      }
      $scope.config = {
        name: $state.params.adapterId,
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


    // METRICS
    var dataSrc = new MyDataSource($scope);
    var filter = $filter('filter');

    var metricParams = {
      namespace: $state.params.namespace,
      app: $state.params.adapterId
    };

    metricParams = MyMetricsQueryHelper.tagsToParams(metricParams);

    var metricBasePath = '/metrics/search?target=metric&' + metricParams;

    var stagesArray = [source].concat(transforms, sinks);
    stagesArray = stagesArray.map(function (n, i) { return n + '.' + (i+1); });


    dataSrc.poll({
      method: 'POST',
      _cdapPath: metricBasePath
    }, function (res) {
      if (res.length > 0) {
        var metricQuery = [];

        angular.forEach(stagesArray, function (node) {
          metricQuery = metricQuery.concat(filter(res, node));
        });

        if (metricQuery.length === 0) { return; }

        dataSrc.request({
          method: 'POST',
          _cdapPath: '/metrics/query?' + metricParams + '&metric=' + metricQuery.join('&metric=')
        }).then(function (metrics) {
          var metricObj = {};

          angular.forEach(metrics.series, function (metric) {
            var split = metric.metricName.split('.');
            var key = split[2] + '.' + split[3];

            if (!metricObj[key]) {
              metricObj[key] = {
                nodeType: split[1],
                nodeName: split[2],
                stage: +split[3]
              };
            }

            if (split[5] === 'in') {
              metricObj[key].recordsIn = metric.data[0].value;
            } else if (split[5] === 'out') {
              metricObj[key].recordsOut = metric.data[0].value;
            }

          });

          var metricsArr = [];
          angular.forEach(metricObj, function (val) {
            metricsArr.push(val);
          });

          $scope.metrics = metricsArr;
        });
      }

    });

  });
