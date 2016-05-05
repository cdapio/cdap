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

angular.module(PKG.name + '.feature.hydratorplusplus')
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
          var metricQuery = [];

          if (res.length > 0) {
            angular.forEach(stagesArray, function (node) {
              metricQuery = metricQuery.concat(filter(res, node));
            });

            if (metricQuery.length === 0) {
              dispatcher.dispatch('onEmptyMetrics');
              return;
            }

            /**
             *  Since the parent block is already a poll, we don't need another poll for
             *  the values of each metrics.
             **/
            dataSrc.request({
              method: 'POST',
              _cdapPath: '/metrics/query?' + metricParams + '&metric=' + metricQuery.join('&metric=')
            }).then(function(metrics) {
              dispatcher.dispatch('onMetricsFetch', metrics);
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

  });
