/*
 * Mapreduce Model
 */

define(['core/lib/date', 'core/models/program'],
  function (Datejs, Program) {

  var METRICS_PATHS = {
    '/reactor/apps/{{appId}}/mapreduce/{{jobId}}/mappers/process.completion?count=30': 'mappersCompletion',
    '/reactor/apps/{{appId}}/mapreduce/{{jobId}}/reducers/process.completion?count=30': 'reducersCompletion',
    '/reactor/apps/{{appId}}/mapreduce/{{jobId}}/mappers/process.entries.in?aggregate=true': 'mappersEntriesIn',
    '/reactor/apps/{{appId}}/mapreduce/{{jobId}}/mappers/process.entries.out?aggregate=true': 'mappersEntriesOut',
    '/reactor/apps/{{appId}}/mapreduce/{{jobId}}/reducers/process.entries.in?aggregate=true': 'reducersEntriesIn',
    '/reactor/apps/{{appId}}/mapreduce/{{jobId}}/reducers/process.entries.out?aggregate=true': 'reducersEntriesOut'
  };

  var METRIC_TYPES = {
    'mappersCompletion': 'number',
    'reducersCompletion': 'number',
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

  var Model = Program.extend({

    href: function () {
      return '#/mapreduce/' + this.get('id');
    }.property('id'),
    metricNames: null,
    instances: 0,
    type: 'Mapreduce',
    plural: 'Mapreduce',
    description: 'Mapreduce',
    startTime: null,

    init: function() {
      this._super();

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

      return this.interpolate('/apps/{parent}/mapreduce/{id}');

    }.property('app', 'name'),

    interpolate: function (path) {

      return path.replace(/\{parent\}/, this.get('app'))
        .replace(/\{id\}/, this.get('name'));

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

    truncatedName: function () {
      return this.get('name').substring(0,6) + '...';
    }.property('name'),

    startStopDisabled: function () {

      if (this.currentState !== 'STOPPED') {
        return true;
      }
      return false;

    }.property('currentState')

  });

  Model.reopenClass({
    type: 'Mapreduce',
    kind: 'Model',
    find: function(model_id, http) {
      var self = this;
      var promise = Ember.Deferred.create();

      var model_id = model_id.split(':');
      var app_id = model_id[0];
      var mapreduce_id = model_id[1];

      http.rest('apps', app_id, 'mapreduce', mapreduce_id, function (model, error) {

        var model = self.transformModel(model);
        model.app = app_id;
        model = C.Mapreduce.create(model);

        http.rest('apps', app_id, 'mapreduce', mapreduce_id, 'status', function (response) {

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
