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
  .service('HydratorPlusPlusDetailMetricsStore', function(HydratorPlusPlusDetailMetricsDispatcher) {

    var dispatcher = HydratorPlusPlusDetailMetricsDispatcher.getDispatcher();
    this.changeListeners = [];
    this.emitChange = function() {
      this.changeListeners.forEach(function(listener) {
        listener(this.state);
      }.bind(this));
    };

    this.setDefaults = function() {
      this.state = {
        metrics: []
      };
      this.emitChange();
    };
    this.setDefaults();
    this.getMetrics = function() {
      return this.state.metrics;
    };
    this.registerOnChangeListener = function(callback) {
      this.changeListeners.push(callback);
    };

    this.emptyMetrics = function () {
      this.state.metrics = [];
      this.emitChange();
    };

    this.setState = function(metrics) {
      var metricObj = {};
      angular.forEach(metrics.series, function (metric) {
        var split = metric.metricName.split('.');
        var key = split[1];

        if (!metricObj[key]) {
          metricObj[key] = {
            nodeName: split[1]
          };
        }

        if (metric.metricName.indexOf(split[1] + '.records.in') !== -1) {
          metricObj[key].recordsIn = metric.data[0].value;
        } else if (metric.metricName.indexOf(split[1] + '.records.out') !== -1) {
          metricObj[key].recordsOut = metric.data[0].value;
        }

      });

      var metricsArr = [];
      angular.forEach(metricObj, function (val) {
        metricsArr.push(val);
      });

      this.state.metrics = metricsArr;
      this.emitChange();
    };
    dispatcher.register('onMetricsFetch', this.setState.bind(this));
    dispatcher.register('onEmptyMetrics', this.emptyMetrics.bind(this));
    dispatcher.register('onReset', this.setDefaults.bind(this));
  });
