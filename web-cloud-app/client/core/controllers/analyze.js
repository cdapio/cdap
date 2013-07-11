/*
 * Analyze Controller.
 * This manages all actions executed from metrics explorer.
 */

define(['../../helpers/chart-helper'], function (chartHelper) {

  // Data for inital load.
  var DEFAULT_FORMAT = 'rate';
  var DEFAULT_DURATION = '7';
  var DEFAULT_METRIC = {
    app: 'Twitter Scanner',
    name: 'Events In',
    color: '#cccccc'
  };

  // Local storage key name.
  var APP_NAME = 'continuuity_analyze';

  // !!Sample apps, must be modified in initalizeDefaults before launch.
  var apps = [
    'Twitter Scanner',
    'App 2',
    'App 3',
  ];

  // !!Sample metrics, must be modified before launch.
  var metricTypes = [
    'Events In',
    'Events Out'
  ];

  var Controller = Em.Controller.extend(Em.Evented, {
    typesBinding: 'model.types',

    load: function (id) {
      this.initalizeDefaults();
      this.uploadPersonalization();

      //Initial load with rate.
      this.updateChart(this.get('format'));
    },

    /**
     * Loads sample data and sets defaults. Must be executed first.
     */
    initalizeDefaults: function() {
      this.set('format', DEFAULT_FORMAT);
      this.set('duration', +DEFAULT_DURATION);
      this.set('data', Em.A());
      this.set('isAddMetricVisible', true);
      this.set('apps', Em.A(apps));
      this.set('metricTypes', Em.A(metricTypes));
      this.set('metricsRequest', {
        appName: DEFAULT_METRIC.app,
        metricType: DEFAULT_METRIC.name,
        color: DEFAULT_METRIC.color
      });
    },

    /**
     * Checks if user has personalization info in local storage or loads default info.
     */
    uploadPersonalization: function() {
      var userData = localStorage.getItem(APP_NAME);
      if (userData) {
        this.set('metrics', JSON.parse(userData)); 
      } else {
        this.set('metrics', Em.A([DEFAULT_METRIC]));
      }
    },

    /**
     * Updates chart data, causes view to rerender chart using on a listener.
     * Forms url based on format and duration and makes a request to fetch appropriate data. Used
     * from the controller to perform any data modifications to chart.
     * @param {string} format chart formats eg: rate or count.
     * @param {number} duration duration of data eg: 7, 14 or 30 days.
     */
    updateChart: function(format, duration) {
      var self = this;
      this.set('data', Em.A());
      var duration = duration || this.get('duration');
      var format = format || this.get('format');
      var metrics = this.get('metrics');
      self.set('format', format);
      self.set('duration', duration);

      for(var i = 0; i < metrics.length; i++) {
        var app = chartHelper.urlRestify(metrics[i]['app']);
        var name = chartHelper.urlRestify(metrics[i]['name']);
        var url = 'metrics/'+app+'/'+name+'?format='+format+'&duration='+duration;

        C.HTTP.get(url, function(status, result) {
            var data = self.get('data');
            var newData = self.extendData(data, result, app, name);
            self.set('data', newData);
          }
        );
      }

    },

    /**
     * Merges existing data and adds a new metric to existing timestamps.
     * @param {Object} data existing chart data.
     * @param {Object} result new metric data.
     * @param {string} app application id eg: twitter scanner.
     * @param {string} name type of metric eg: events in, tuples processed.
     * @return {Object} modified data object.
     */
    extendData: function(data, result, app, name) {

      // If data doesn't exist, store based on new key. Used for initial load.
      if (!data.length)
        return result.map(function(item) {
          var obj = {};
          obj['timestamp'] = item.timestamp;
          obj[chartHelper.getAppName(app, name)] = item.value;
          return obj;
        });

      var newData = [];
      for (var j = 0, jLen = data.length; j < jLen; j++) {
        for (var k = 0, kLen = result.length; k < kLen; k++) {

          // Check timestamps match before merging data.
          if(result[k].timestamp === data[j].timestamp) {
            var obj = $.extend(true, {}, data[j]);
            obj['timestamp'] = data[j].timestamp;
            obj[chartHelper.getAppName(app, name)] = result[k].value;

            newData.pushObject(obj);
          }
        }
      }
      return newData;
    },

    /**
     * Adds a new metric to chart.
     * Requires metricsRequest.appName, metricsRequest.metricType to be set before addding.
     * @return {null} if metric already exists.
     */
    addMetric: function() {
      if (this.metricExists(
        this.get('metricsRequest.appName'), this.get('metricsRequest.metricType')))
        return;
      this.get('metrics').pushObject({
        app: this.get('metricsRequest.appName'),
        name: this.get('metricsRequest.metricType'),
        color: this.get('metricsRequest.color')
      });

      this.updateChart(this.get('format'));
    },

    /**
     * Removes a metric from chart.
     * @param {string} app application id eg: twitter scanner.
     * @param {string} name type of metric eg: events in, tuples processed.
     */
    removeMetric: function(app, name) {
      var self = this;
      var metrics = this.get('metrics');
      var data = this.get('data');
      metrics = metrics.filter(function(item) {
        return !(item.app == app && item.name == name);
      });
      this.set('metrics',  metrics);

      var modifiedName = chartHelper.getAppName(
        chartHelper.urlRestify(app), chartHelper.urlRestify(name));
      data = data.filter(function(item) {
        delete item[modifiedName];
        return item;
      });
      self.set('data', data);
    },

    /**
     * Toggles add metric dialog based on current state. Display/hide is done at the view level.
     */
    toggleAddMetricDialog: function() {
      this.set('isAddMetricVisible', !this.get('isAddMetricVisible'));
    },

    /**
     * Sets app name for a new add metric request.
     * @param {string} appName name of application.
     */
    setAppRequest: function(appName) {
      this.set('metricsRequest.appName', appName.toString());
    },

    /**
     * Sets metric type for new add metric request.
     * @param {string} metricType type of metric request.
     */
    setMetricTypeRequest: function(metricType) {
      this.set('metricsRequest.metricType', metricType.toString());
    },

    /**
     * Checks if metric with app and name already exist in application.
     * @param {string} app application id eg: twitter scanner.
     * @param {string} name type of metric eg: events in, tuples processed.
     * @return {boolen} whether metric exists.
     */
    metricExists: function(app, name) {
      var metrics = this.get('metrics');

      for(var i = 0, len = metrics.length; i < len; i++) {
        if (app === metrics[i].app && name === metrics[i].name) {
          return true;
        }
      }
      return false;
    },

    /**
     * Saves existing application state to localstorage.
     */
    saveToStore: function() {
      localStorage.setItem(APP_NAME, JSON.stringify(this.get('metrics')));
    }.observes('metrics.[]')

  });

  Controller.reopenClass({
    type: 'Analyze',
    kind: 'Controller'
  });

  return Controller;

});