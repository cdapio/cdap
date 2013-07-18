/*
 * Batch Model
 */

define(['lib/date'], function (Datejs) {

  var METRICS_PATHS = {
    '/process/busyness/{{appId}}/mapreduce/{{jobId}}?count=30': 'busyness',
    '/process/completion/{{appId}}/mapreduce/{{jobId}}/mappers?aggregate=true': 'mappersCompletion',
    '/process/completion/{{appId}}/mapreduce/{{jobId}}/reducers?aggregate=true': 'reducersCompletion',
    '/process/bytes/{{appId}}/mapreduce/{{jobId}}/mappers?count=30': 'mappersBytesProcessed',
    '/process/entries/{{appId}}/mapreduce/{{jobId}}/mappers/ins?aggregate=true': 'mappersEntriesIn',
    '/process/entries/{{appId}}/mapreduce/{{jobId}}/mappers/outs?aggregate=true': 'mappersEntriesOut',
    '/process/entries/{{appId}}/mapreduce/{{jobId}}/reducers/ins?aggregate=true': 'reducersEntriesIn',
    '/process/entries/{{appId}}/mapreduce/{{jobId}}/reducers/outs?aggregate=true': 'reducersEntriesOut',
  };

  var METRIC_TYPES = {
    'busyness': 'number',
    'mappersCompletion': 'float',
    'reducersCompletion': 'float',
    'mappersBytesProcessed': 'bytes',
    'mappersEntriesIn': 'number',
    'mappersEntriesOut': 'number',
    'reducersEntriesIn': 'number',
    'reducersEntriesOut': 'number'
  };

  var Model = Em.Object.extend({

    href: function () {
      return '#/batches/' + this.get('id');
    }.property('id'),
    metricNames: null,
    __loadingData: false,
    instances: 0,
    type: 'Batch',
    plural: 'Batches',
    startTime: null,

    init: function() {
      this._super();
      this.set('metricData', Em.Object.create({
        busyness: 0,
        mappersCompletion: 10,
        reducersCompletion: 0,
        mappersBytesProcessed: 0,
        mappersEntriesIn: 0,
        mappersEntriesOut: 0,
        reducersEntriesIn: 0,
        reducersEntriesOut: 0
      }));
      this.set('metricNames', {});

      this.set('name', (this.get('flowId') || this.get('id') || this.get('meta').name));

      this.set('app', this.get('applicationId') || this.get('application'));
      this.set('id', this.get('app') + ':' +
        (this.get('flowId') || this.get('id') || this.get('meta').name));

      if (this.get('meta')) {
        this.set('startTime', this.get('meta').startTime);
      }
    },

    getStartDate: function() {
      var time = parseInt(this.get('startTime'), 10);
      return new Date(time).toString('MMM d, yyyy');
    }.property('startTime'),

    getStartHours: function() {
      var time = parseInt(this.get('startTime'), 10);
      return new Date(time).toString('hh:mm tt');
    }.property('startTime'),

    addMetricName: function (name) {

      this.get('metricNames')[name] = 1;

    },

    setMetricData: function(name, value) {

      this.get('metricData').set(name, value);

    },

    getUpdateRequest: function (http) {

      var self = this;

      var app_id = this.get('app'),
        batch_id = this.get('name'),
        start = C.__timeRange * -1;

      var metrics = [];
      var metricNames = this.get('metricNames');
      for (var name in metricNames) {
        if (metricNames[name] === 1) {
          metrics.push(name);
        }
      }
      if (!metrics.length) {
        this.set('__loadingData', false);
        return;
      }

      http.rpc('runnable', 'status', [app_id, batch_id, -1],
        function (response) {

          if (response.result) {
            self.set('currentState', response.result.status);
          }

      });

      return ['monitor', {
        method: 'getTimeSeries',
        params: [app_id, batch_id, metrics, start, undefined, 'FLOW_LEVEL']
      }, function (error, response) {

        if (!response.params) {
          return;
        }

        var data, points = response.params.points,
          latest = response.params.latest;

        for (var metric in points) {
          data = points[metric];

          var k = data.length;

          while(k --) {
            data[k] = data[k].value;
          }

          metric = metric.replace(/\./g, '');

          self.get('metricData').set(metric, data);
          self.set('__loadingData', false);
        }

      }];

    },

    getMetricsRequest: function(http) {

      var appId = this.get('app');
      var jobId = this.get('name');
      var datasetId = this.get('datasets')[0];

      var paths = [];
      var pathMap = {};
      for (var path in METRICS_PATHS) {
        var url = S(path).template({'appId': appId, 'jobId': jobId}).s;
        paths.push(url);
        pathMap[url] = METRICS_PATHS[path];
      }

      var self = this;

      http.post('metrics', paths, function(response, status) {

        if(!response.result) {
          return;
        }

        var result = response.result;
        var i = result.length, metric;
        
        while (i--) {

          metric = pathMap[result[i]['path']];
          if (metric) {

            if (result[i]['value'] instanceof Array) {
              result[i]['value'] = result[i]['value'].map(function (entry) {
                return entry.value;
              });
              self.setMetricData(metric, result[i]['value']);
            }
            else if (metric in METRIC_TYPES && METRIC_TYPES[metric] == 'number') {
              self.setMetricData(metric, C.Util.number(result[i]['value']));
            } else {
              self.setMetricData(metric, result[i]['value']);
            }

          }
          metric = null;
        }

      });

    },

    getMeta: function () {
      var arr = [];
      for (var m in this.meta) {
        arr.push({
          k: m,
          v: this.meta[m]
        });
      }
      return arr;
    }.property('meta'),

    isRunning: function() {

      return this.get('currentState') === 'RUNNING';

    }.property('currentState'),

    started: function () {
      return this.lastStarted >= 0 ? $.timeago(this.lastStarted) : 'No Date';
    }.property('timeTrigger'),

    stopped: function () {
      return this.lastStopped >= 0 ? $.timeago(this.lastStopped) : 'No Date';
    }.property('timeTrigger'),

    actionIcon: function () {

      if (this.currentState === 'RUNNING' ||
        this.currentState === 'PAUSING') {
        return 'btn-pause';
      } else {
        return 'btn-start';
      }

    }.property('currentState').cacheable(false),

    stopDisabled: function () {

      if (this.currentState === 'RUNNING') {
        return false;
      }
      return true;

    }.property('currentState'),

    startPauseDisabled: function () {

      if (this.currentState !== 'STOPPED' &&
        this.currentState !== 'PAUSED' &&
        this.currentState !== 'DEPLOYED' &&
        this.currentState !== 'RUNNING') {
        return true;
      }
      return false;

    }.property('currentState'),

    defaultAction: function () {
      return {
        'deployed': 'Start',
        'stopped': 'Start',
        'stopping': 'Start',
        'starting': 'Start',
        'running': 'Pause',
        'adjusting': '...',
        'draining': '...',
        'failed': 'Start'
      }[this.currentState.toLowerCase()];
    }.property('currentState')
  });

  Model.reopenClass({
    type: 'Batch',
    kind: 'Model',
    find: function(model_id, http) {

      var promise = Ember.Deferred.create();

      var model_id = model_id.split(':');
      var app_id = model_id[0];
      var mapreduce_id = model_id[1];

      http.rest('apps', app_id, 'mapreduce', mapreduce_id, function (model, error) {

        model = C.Batch.create(model);
        http.rpc('runnable', 'status', [app_id, mapreduce_id, -1],
          function (response) {

            if (response.error) {
              promise.reject(response.error);
            } else {
              model.set('currentState', response.result.status);
              promise.resolve(model);
            }

        });

      });

      return promise;

    }
  });

  return Model;

});
