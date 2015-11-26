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
  .service('PipelineDetailMetricsActionFactory', function(DetailRunsStore, PipelineDetailMetricslDispatcher, MyCDAPDataSource, $filter, MyMetricsQueryHelper, DetailNonRunsStore) {

    var dispatcher = PipelineDetailMetricslDispatcher.getDispatcher();
    var metricsPollId;
    var metricValuesPollId;
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
      var api;
      metricParams = MyMetricsQueryHelper.tagsToParams(metricParams);
      var metricBasePath = '/metrics/search?target=metric&' + metricParams;
      if (isPoll) {
        api = dataSrc.poll.bind(dataSrc);
      } else {
        api = dataSrc.request.bind(dataSrc);
      }
      metricsPollId = api({
        method: 'POST',
        _cdapPath: metricBasePath
      }, function (res) {
        var config = DetailNonRunsStore.getConfigJson();
        var source = config.source.name;
        var transforms = config.transforms.map(function (n) { return n.name; });
        var sinks = config.sinks.map(function (n) { return n.name; });
        var stagesArray = [source].concat(transforms, sinks);
        stagesArray = stagesArray.map(function (n, i) { return n + '.' + (i+1); });
        var metricQuery = [];

        if (res.length > 0) {
          angular.forEach(stagesArray, function (node) {
            metricQuery = metricQuery.concat(filter(res, node));
          });

          if (metricQuery.length === 0) { return; }
          this.stopMetricValuesPoll();

          metricValuesPollId = api({
            method: 'POST',
            _cdapPath: '/metrics/query?' + metricParams + '&metric=' + metricQuery.join('&metric=')
          }, function(metrics) {
            dispatcher.dispatch('onMetricsFetch', metrics);
          });

          metricValuesPollId = metricValuesPollId.__pollId__;
        }
      }.bind(this));

      metricsPollId = metricsPollId.__pollId__;
    }

    this.stopMetricsPoll = function() {
      if (metricsPollId) {
        dataSrc.stopPoll(metricsPollId);
        metricsPollId = null;
      }
    };

    this.stopMetricValuesPoll = function() {
      if (metricValuesPollId) {
        dataSrc.stopPoll(metricValuesPollId);
        metricValuesPollId = null;
      }
    };

    this.reset = function() {
      this.stopMetricsPoll();
      this.stopMetricValuesPoll();
      dispatcher.dispatch('onReset');
    };

  });
