/*
 * Analyze Controller
 */

define([], function (chartHelper) {

  var DEFAULT_FORMAT = 'rate';
  var DEFAULT_DURATION = '7';
  var DEFAULT_METRIC = {
    app: 'Twitter Scanner',
    name: 'Events In'
  };

  var apps = [
    'Twitter Scanner',
    'App 2',
    'App 3',
  ];

  var metricTypes = [
    'Events In',
    'Events Out'
  ];

  var Controller = Em.Controller.extend(Em.Evented, {
    typesBinding: 'model.types',

    load: function (id) {
      this.set('format', DEFAULT_FORMAT);
      this.set('duration', +DEFAULT_DURATION);
      this.set('metrics', Em.A([DEFAULT_METRIC]));
      this.set('data', Em.A());
      this.set('isAddMetricVisible', true);
      this.set('apps', Em.A(apps));
      this.set('metricTypes', Em.A(metricTypes));
      this.set('metricsRequest', {
        appName: DEFAULT_METRIC.app,
        metricType: DEFAULT_METRIC.name
      });
      //Initial load with rate.
      this.changeMetricFormat(this.get('format'));
    },

    changeMetricFormat: function(formatType) {
      var self = this;
      var duration = this.get('duration')
      C.HTTP.get('metrics/twitter_scanner/events_in?format='+formatType+'&duration='+duration,
        function(status, result) {
          self.set('data', result);
          self.set('format', formatType);
        }
      );
    },

    changeMetricDuration: function(duration) {
      var self = this;
      var format = this.get('format');
      C.HTTP.get('metrics/twitter_scanner/events_in?format='+format+'&duration='+duration,
        function(status, result) {
          self.set('data', result);
          self.set('duration', +duration)
        }
      );
    },

    toggleAddMetricDialog: function() {
      this.set('isAddMetricVisible', !this.get('isAddMetricVisible'));
    },

    setDefaultView: function() {
      this.set('duration', +DEFAULT_DURATION);
      this.set('format', DEFAULT_FORMAT);
      this.changeMetricFormat(this.get('format'));
    },

    setAppRequest: function(appName) {
      this.set('metricsRequest.appName', appName.toString());
    },

    setMetricTypeRequest: function(metricType) {
      this.set('metricsRequest.metricType', metricType.toString());
    },

    addMetric: function() {
      if (this.metricExists())
        return;
      this.get('metrics').pushObject({
        app: this.get('metricsRequest.appName'),
        name: this.get('metricsRequest.metricType')
      });
      var self = this;
      var data = self.get('data');
      var newData = [];
      C.HTTP.get('metrics/twitter_scanner/events_out?format=rate&duration=7',
        function(status, result) {
          for (var j = 0; j < data.length; j++) {
            for (var k = 0; k < result.length; k++) {
              if(result[k].timestamp === data[j].timestamp) {
                newData.pushObject({
                  timestamp: data[j].timestamp,
                  value: data[j].value,
                  eventsOut: result[k].value
                });  
              }
              
            }
          }
          console.log(newData);
          self.set('data', newData);
        }
      );
    },

    metricExists: function() {
      var metrics = this.get('metrics');

      for(var i=0; i < metrics.length; i++) {
        if (this.get('metricsRequest.appName') === metrics[i].app
          && this.get('metricsRequest.metricType') === metrics[i].name) {
          return true;
        }
      }
      return false;
    },

    unload: function () {

    },

    delete: function () {

    }

  });

  Controller.reopenClass({
    type: 'Analyze',
    kind: 'Controller'
  });

  return Controller;

});