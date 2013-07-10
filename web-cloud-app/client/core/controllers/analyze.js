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
      this.set('data', Em.A());
      var duration = this.get('duration');
      var metrics = this.get('metrics');
      self.set('format', formatType);

      for(var i = 0; i < metrics.length; i++) {
        var app = self.urlRestify(metrics[i]['app']);
        var name = self.urlRestify(metrics[i]['name']);
        var url = 'metrics/'+app+'/'+name+'?format='+formatType+'&duration='+duration;

        C.HTTP.get(url, function(status, result) {
            var data = self.get('data');
            var newData = self.extendData(data, result);
            self.set('data', newData);
          }
        );
      }

    },

    changeMetricDuration: function(duration) {
      var self = this;
      this.set('data', Em.A());
      var format = this.get('format');
      var metrics = this.get('metrics');
      self.set('duration', +duration);

      for(var i = 0; i < metrics.length; i++) {
        var app = self.urlRestify(metrics[i]['app']);
        var name = self.urlRestify(metrics[i]['name']);
        var url = 'metrics/'+app+'/'+name+'?format='+format+'&duration='+duration;

        C.HTTP.get(url, function(status, result) {
            var data = self.get('data');
            var newData = self.extendData(data, result);
            self.set('data', newData);
          }
        );
      }

    },

    removeMetric: function(app, name) {
      var self = this;
      var metrics = this.get('metrics');
      var data = this.get('data');
      metrics = metrics.filter(function(item) {
        return !(item.app == app && item.name == name);
      });
      this.set('metrics',  metrics);
      console.log(data);
      var modifiedName = self.urlRestify(name);
      data = data.filter(function(item) {
        delete item['eventsOut'];
        return item;
      });
      self.set('data', data);
    },

    urlRestify: function(word) {
      return word.split(' ').map(function(segment) {
        return segment.toLowerCase();
      }).join('_');
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
      this.changeMetricFormat(this.get('format'));
      var self = this;

    },

    extendData: function(data, result) {
      if (!data.length)
        return result;
      var newData = [];
      for (var j = 0, jLen = data.length; j < jLen; j++) {
        for (var k = 0, kLen = result.length; k < kLen; k++) {
          if(result[k].timestamp === data[j].timestamp) {
            newData.pushObject({
              timestamp: data[j].timestamp,
              value: data[j].value,
              eventsOut: result[k].value
            });
          }
        }
      }
      return newData;
    },

    metricExists: function() {
      var metrics = this.get('metrics');

      for(var i = 0, len = metrics.length; i < len; i++) {
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