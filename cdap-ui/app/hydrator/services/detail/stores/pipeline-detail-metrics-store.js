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
        metrics: [],
        metricsTabActive: false,
        logs: {}
      };
      this.emitChange();
    };
    this.setDefaults();
    this.getMetrics = function() {
      return this.state.metrics;
    };
    this.getLogsMetrics = function() {
      return this.state.logs;
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
      var systemLogMetrics = [
        'system.app.log.error',
        'system.app.log.warn'
      ];
      var logsMetrics = {};
      angular.forEach(metrics.series, function (metric) {
        var split = metric.metricName.split('.');
        var key = split[1];

        if (key !== 'app' && !metricObj[key]) {
          metricObj[key] = {
            nodeName: key
          };
        }

        let metricName = metric.metricName;
        let metricValue = metric.data[0].value;

        if (metricName.indexOf(key + '.records.in') !== -1) {
          metricObj[key].recordsIn = metricValue;
        } else if (metricName.indexOf(key + '.records.out') !== -1) {

          // contains multiple records.out metrics
          if (metricName.indexOf(key + '.records.out.') !== -1) {
            let port = split[split.length - 1];
            if (!metricObj[key].recordsOut) {
              metricObj[key].recordsOut = {};
            }
            metricObj[key].recordsOut[port] = metricValue;
          } else {
            metricObj[key].recordsOut = metricValue;
          }

        } else if (metricName.indexOf(key + '.records.error') !== -1) {
          metricObj[key].recordsError = metricValue;
        } else if (systemLogMetrics.indexOf(metricName) !== -1) {
          logsMetrics[metricName] = metricValue;
        }

      });

      var metricsArr = [];
      angular.forEach(metricObj, function (val) {
        metricsArr.push(val);
      });
      this.state.logs = logsMetrics;

      this.state.metrics = metricsArr;
      this.emitChange();
    };

    this.setActiveTab = function(active) {
      this.state.metricsTabActive = active;
    };

    dispatcher.register('onMetricsFetch', this.setState.bind(this));
    dispatcher.register('onEmptyMetrics', this.emptyMetrics.bind(this));
    dispatcher.register('onReset', this.setDefaults.bind(this));
    dispatcher.register('onSetMetricsTabActive', this.setActiveTab.bind(this));
  });
