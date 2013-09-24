/*
 * Batch Model
 */

define(['core/lib/date'], function (Datejs) {

  var METRICS_PATHS = {
    //'/process/busyness/{{appId}}/mapreduces/{{jobId}}?count=30': 'busyness',
    '/process/completion/{{appId}}/mapreduces/{{jobId}}/mappers?count=30': 'mappersCompletion',
    '/process/completion/{{appId}}/mapreduces/{{jobId}}/reducers?count=30': 'reducersCompletion',
    //'/process/bytes/{{appId}}/mapreduces/{{jobId}}/mappers?count=30': 'mappersBytesProcessed',
    '/process/entries/{{appId}}/mapreduces/{{jobId}}/mappers/ins?aggregate=true': 'mappersEntriesIn',
    '/process/entries/{{appId}}/mapreduces/{{jobId}}/mappers/outs?aggregate=true': 'mappersEntriesOut',
    '/process/entries/{{appId}}/mapreduces/{{jobId}}/reducers/ins?aggregate=true': 'reducersEntriesIn',
    '/process/entries/{{appId}}/mapreduces/{{jobId}}/reducers/outs?aggregate=true': 'reducersEntriesOut'
  };

  var METRIC_TYPES = {
    //'busyness': 'number',
    'mappersCompletion': 'number',
    'reducersCompletion': 'number',
    //'mappersBytesProcessed': 'bytes',
    'mappersEntriesIn': 'number',
    'mappersEntriesOut': 'number',
    'reducersEntriesIn': 'number',
    'reducersEntriesOut': 'number'
  };

  var EXPECTED_FIELDS = [
    'divId',
    'name',
    'description',
    'datasets',
    'inputDataSet',
    'outputDataSet'
  ];

  var Model = Em.Object.extend({

    href: function () {
      return '#/batches/' + this.get('id');
    }.property('id'),
    metricNames: null,
    instances: 0,
    type: 'Batch',
    plural: 'Batches',
    startTime: null,

    init: function() {
      this._super();

      this.set('timeseries', Em.Object.create());
      this.set('aggregates', Em.Object.create());
      this.set('currents', Em.Object.create());

      this.set('metricData', Em.Object.create({
        busyness: 0,
        mappersCompletion: 0,
        reducersCompletion: 0,
        mappersBytesProcessed: 0,
        mappersEntriesIn: 0,
        mappersEntriesOut: 0,
        reducersEntriesIn: 0,
        reducersEntriesOut: 0
      }));
      this.set('metricNames', {});

      this.set('app', this.get('app') || this.get('applicationId') || this.get('appId'));

      this.set('id', this.get('app') + ':' + this.get('name'));
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

    /*
     * Runnable context path, used by user-defined metrics.
     */
    context: function () {

      return this.interpolate('/apps/{parent}/flows/{id}');

    }.property('app', 'name'),

    interpolate: function (path) {

      return path.replace(/\{parent\}/, this.get('app'))
        .replace(/\{id\}/, this.get('name'));

    },

    trackMetric: function (path, kind, label) {

      path = this.interpolate(path);
      this.get(kind).set(C.Util.enc(path), Em.Object.create({
        path: path,
        value: label || []
      }));
      return path;

    },

    setMetric: function (label, value) {

      var unit = this.get('units')[label];
      value = C.Util[unit](value);

      this.set(label + 'Label', value[0]);
      this.set(label + 'Units', value[1]);

    },

    units: {
      'events': 'number',
      'storage': 'bytes',
      'containers': 'number',
      'cores': 'number'
    },

    updateState: function (http) {

      var self = this;

      var app_id = this.get('app'),
        mapreduce_id = this.get('name');

      http.rest('apps', app_id, 'mapreduces', mapreduce_id, 'status', function (response) {

          if (!$.isEmptyObject(response)) {
            self.set('currentState', response.status);
          }
        });
    },

    getMetricsRequest: function(http) {

      var appId = this.get('app');
      var jobId = this.get('name');
      var datasetId = this.get('inputDataSet');

      var paths = [];
      var pathMap = {};
      for (var path in METRICS_PATHS) {
        var url = new S(path).template({'appId': appId, 'jobId': jobId}).s;
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

            if (result[i]['result']['data'] instanceof Array) {

              result[i]['result']['data'] = result[i]['result']['data'].map(function (entry) {
                return entry.value;
              });

              // Hax for current value of completion.
              if (metric === 'mappersCompletion' ||
                  metric === 'reducersCompletion') {

                var data = result[i]['result']['data'];

                self.setMetricData(metric, data[data.length - 1]);

              } else {

                self.setMetricData(metric, result[i]['result']['data']);

              }

            }
            else if (metric in METRIC_TYPES && METRIC_TYPES[metric] === 'number') {

              self.setMetricData(metric, C.Util.numberArrayToString(result[i]['result']['data']));

            } else {

              self.setMetricData(metric, result[i]['result']['data']);

            }

          }
          metric = null;
        }

      });

    },

    totalCompletion: function () {
      return (this.get('metricData.mappersCompletion') +
       this.get('metricData.reducersCompletion')) / 200;

    }.observes('metricData.mappersCompletion', 'metricData.reducersCompletion').property('metricData.mappersCompletion', 'metricData.reducersCompletion'),


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

      if (!this.currentState) {
        return '...';
      }

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
    }.property('currentState'),

    truncatedName: function () {
      return this.get('name').substring(0,6) + '...';
    }.property('name')

  });

  Model.reopenClass({
    type: 'Batch',
    kind: 'Model',
    find: function(model_id, http) {
      var self = this;
      var promise = Ember.Deferred.create();

      var model_id = model_id.split(':');
      var app_id = model_id[0];
      var mapreduce_id = model_id[1];

      http.rest('apps', app_id, 'mapreduces', mapreduce_id, function (model, error) {
        var model = self.transformModel(model);
        model.app = app_id;
        model = C.Batch.create(model);
        http.rest('apps', app_id, 'mapreduces', mapreduce_id, 'status', function (response) {

          if ($.isEmptyObject(response)) {
            promise.reject('Status could not retrieved.');
          } else {
            model.set('currentState', response.status);
            promise.resolve(model);
          }

        });

      });

      return promise;

    },

    transformModel: function (model) {

      var newModel = {};
      for (var i = EXPECTED_FIELDS.length - 1; i >= 0; i--) {
        newModel[EXPECTED_FIELDS[i]] = model[EXPECTED_FIELDS[i]];
      }
      if ('appId' in model || 'applicationId' in model) {
        newModel.appId = model.appId || model.applicationId;
      }
      return newModel;

    }
  });

  return Model;

});
