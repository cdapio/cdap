/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
  .service('HydratorPlusPlusDetailMetricsActions', function(HydratorPlusPlusDetailMetricsDispatcher, MyCDAPDataSource, $filter, MyMetricsQueryHelper, HydratorPlusPlusDetailNonRunsStore, $interval) {

    var dispatcher = HydratorPlusPlusDetailMetricsDispatcher.getDispatcher();
    var metricsPoll;
    // FIXME: This is a memory leak. We need to fix this.
    var dataSrc = new MyCDAPDataSource();
    var filter = $filter('filter');
    this.pollForMetrics = function(params) {
      this.stopMetricsPoll();
      getMetrics.call(this, params, true);
    };

    this.requestForMetrics = function (params) {
      getMetrics.call(this, params, false);
    };

    function getMetrics(params, isPoll) {
      var metricParams = params;
      metricParams = MyMetricsQueryHelper.tagsToParams(metricParams);
      var metricBasePath = '/metrics/search?target=metric&' + metricParams;

      function request() {
        dataSrc.request({
          method: 'POST',
          _cdapPath: metricBasePath
        }).then(function (res) {
          var config = HydratorPlusPlusDetailNonRunsStore.getConfigJson();
          var stagesArray, source, sinks, transforms;
          if (config.stages) {
            stagesArray = config.stages.map(n => n.name);
          } else {
            source = config.source.name;
            transforms = config.transforms.map(function (n) { return n.name; });
            sinks = config.sinks.map(function (n) { return n.name; });
            stagesArray = [source].concat(transforms, sinks);
          }
          var metricQuery = ['system.app.log.error', 'system.app.log.warn'];

          if (res.length > 0) {
            angular.forEach(stagesArray, function (node) {
              // Prefixing it with user. as we to filter out only user metrics and not system metrics
              // This was a problem if a node name is a substring of a system metric. Ref: CDAP-12121
              metricQuery = metricQuery.concat(filter(res, 'user.' + node));
            });

            /**
             *  Since the parent block is already a poll, we don't need another poll for
             *  the values of each metrics.
             **/
            dataSrc.request({
              method: 'POST',
              _cdapPath: '/metrics/query',
              body: {
                qid: {
                  tags: params,
                  metrics: metricQuery,
                  aggregate: true,
                  timeRange: {
                    startTime: 0,
                    endTime: 'now'
                  }
                }
              }
            }).then(function(metrics) {
              dispatcher.dispatch('onMetricsFetch', metrics.qid);
            });
          }
        }.bind(this));

      }

      /**
       * FIXME:
       * The reason we are using interval here is because the node server does not send result
       * if there is no change in data. Since we were only polling for the list of available metrics
       * there won't be change of data, thus the metrics is not updating.
       **/
      if (isPoll) {
        request();
        metricsPoll = $interval(request, 10000);
      } else {
        request();
      }

    }

    this.stopMetricsPoll = function() {
      if (metricsPoll) {
        $interval.cancel(metricsPoll);
        metricsPoll = null;
      }
    };

    this.reset = function() {
      this.stopMetricsPoll();
      dispatcher.dispatch('onReset');
    };

    this.setMetricsTabActive = function(active) {
      dispatcher.dispatch('onSetMetricsTabActive', active);
    };

  });
