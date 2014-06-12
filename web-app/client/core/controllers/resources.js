/*
 * Resources Controller
 */

define([], function () {

  var DASH_CHART_COUNT = 60;

  var MEGABYTES = 1024 * 1024;

  var Controller = Em.Controller.extend({

    structure: { text: 'Root', children: Em.ArrayProxy.create({ content: [] }) },
    elements: Em.Object.create(),

    currents: Em.Object.create(),
    timeseries: Em.Object.create(),

    value: Em.Object.create(),
    total: Em.Object.create(),

    load: function () {

      this.clearTriggers(true);

      this.set('model', Em.Object.create({
        addMetricName: function (metric) {
          this.get('metricNames')[metric] = 1;
        },
        metricNames: {},
        metricData: Em.Object.create()
      }));

      var self = this;

      this.set('structure', { text: 'Root', children: Em.ArrayProxy.create({ content: [] }) });

      this.set('elements.App', Em.ArrayProxy.create({ content: [] }));
      this.set('elements.Flow', Em.ArrayProxy.create({ content: [] }));
      this.set('elements.Flowlet', Em.ArrayProxy.create({ content: [] }));
      this.set('elements.Mapreduce', Em.ArrayProxy.create({ content: [] }));
      this.set('elements.Workflow', Em.ArrayProxy.create({ content: [] }));
      this.set('elements.Procedure', Em.ArrayProxy.create({ content: [] }));

      var now = new Date().getTime();
      var start = now - ((30) * 1000);
      start = Math.floor(start / 1000);

      this.HTTP.post('metrics', [
        '/reactor/cluster/resources.total.memory?start=' + start + '&count=1&interpolate=step',
        '/reactor/cluster/resources.total.storage?start=' + start + '&count=1&interpolate=step'
        ], function (response) {

        if (response.result && response.result[0].result.data.length) {
          var result = response.result;

          var memory = result[0].result.data[0].value * MEGABYTES;
          memory = C.Util.bytes(memory);
          self.set('total.memory', {
            label: memory[0],
            unit: memory[1]
          });

          var storage = result[1].result.data[0].value * MEGABYTES;
          storage = C.Util.bytes(storage);
          self.set('total.storage', {
            label: storage[0],
            unit: storage[1]
          });

        } else {
          self.set('total.memory', {
            label: 0,
            unit: 'B'
          });
          self.set('total.storage', {
            label: 0,
            unit: 'B'
          });

        }

        self.HTTP.rest('apps', function (objects) {

          var structure = self.get('structure');
          var elements = self.get('elements');

          function next (object) {

            object.children = object.children || Em.ArrayProxy.create({ content: [] });

            if (typeof object.getSubPrograms === 'function') {

              object.getSubPrograms(function (programs) {

                for (var type in programs) {

                  // Push list into entity cache.
                  elements[type].pushObjects(programs[type]);

                  var program, context;

                  for (var i = 0; i < programs[type].length; i ++) {

                    program = programs[type][i];
                    context = '/reactor' + program.get('context') + '/';

                    program.trackMetric(context + 'resources.used.memory', 'timeseries', null, true);
                    program.trackMetric(context + 'resources.used.containers', 'currents', 'containers');
                    program.trackMetric(context + 'resources.used.vcores', 'currents', 'cores');

                    // Tells the template-embedded chart which metric to render.
                    program.set('pleaseObserve', context + 'resources.used.memory');

                    object.children.pushObject(program);
                    next(program);

                  }

                }

              }, self.HTTP);

            }

          }

          for (var i = 0; i < objects.length; i ++) {
            objects[i] = C.App.create(objects[i]);
            structure.children.pushObject(objects[i]);
          }

          next({
            getSubPrograms: function (callback) {
              callback({
                'App': objects
              });
            }
          });

          /*
           * Give the chart Embeddables 100ms to configure
           * themselves before updating.
           */
          setTimeout(function () {
            self.updateStats();
          }, C.EMBEDDABLE_DELAY);

          self.interval = setInterval(function () {
            self.updateStats();
          }, C.POLLING_INTERVAL);

        });

      });

    },

    unload: function () {

      clearInterval(this.interval);
      this.set('elements', Em.Object.create());
      this.set('counts', Em.Object.create());

      this.set('currents', Em.Object.create());
      this.set('timeseries', Em.Object.create());

    },

    ajaxCompleted: function () {
      return this.get('timeseriesCompleted') && this.get('aggregatesCompleted') && this.get('miscCompleted');
    },

    clearTriggers: function (value) {
      this.set('timeseriesCompleted', value);
      this.set('aggregatesCompleted', value);
      this.set('miscCompleted', value);
    },

    updateStats: function () {

      if (!this.ajaxCompleted() || C.currentPath !== 'Resources') {
        return;
      }

      var self = this, types = ['App', 'Flow', 'Flowlet', 'Mapreduce', 'Workflow', 'Procedure'];

      var i, models = [];
      for (i = 0; i < types.length; i ++) {
        models = models.concat(this.get('elements').get(types[i]).get('content'));
      }

      var now = new Date().getTime();

      var start = 'now-' + (C.__timeRange + C.RESOURCE_METRICS_BUFFER) + 's';
      var end = 'now-' + C.RESOURCE_METRICS_BUFFER + 's';

      this.clearTriggers(false);

      // Scans models for timeseries metrics and updates them.
      C.Util.updateTimeSeries(models, this.HTTP, this, C.RESOURCE_METRICS_BUFFER);

      // Scans models for current metrics and updates them.
      C.Util.updateCurrents(models, this.HTTP, this, C.RESOURCE_METRICS_BUFFER);

      // Hax. Count is timerange because server treats end = start + count (no downsample yet)
      var queries = [
        '/reactor/resources.used.memory?count=' + C.__timeRange + '&start=' + start + '&end=' +
          end + '&interpolate=step',
        '/reactor/resources.used.containers?count=' + C.__timeRange + '&start=' + start + '&end=' +
          end +'&interpolate=step',
        '/reactor/resources.used.vcores?count=' + C.__timeRange + '&start=' + start + '&end=' +
          end + '&interpolate=step',
        '/reactor/resources.used.storage?count=' + C.__timeRange + '&start=' + start + '&end=' +
          end + '&interpolate=step'
      ], self = this;

      function lastValue(arr) {
        return arr[arr.length - 1].value;
      }

      this.HTTP.post('metrics', queries, function (response) {
        self.set('miscCompleted', true);
        if (response.result) {

          var result = response.result;

          var i = result[0].result.data.length;
          while (i--) {
            result[0].result.data[i].value = result[0].result.data[i].value * MEGABYTES;
          }
          i = result[3].result.data.length;
          while (i--) {
            result[3].result.data[i].value = result[3].result.data[i].value * MEGABYTES;
          }

          self.set('timeseries.memory', result[0].result.data);
          self.set('timeseries.containers', result[1].result.data);
          self.set('timeseries.cores', result[2].result.data);
          self.set('timeseries.storage', result[3].result.data);

          var memory = C.Util.bytes(lastValue(result[0].result.data));
          self.set('value.memory', {
            label: memory[0],
            unit: memory[1]
          });

          self.set('value.containers', lastValue(result[1].result.data));
          self.set('value.cores', lastValue(result[2].result.data));

          var storage = C.Util.bytes(lastValue(result[3].result.data));
          self.set('value.storage', {
            label: storage[0],
            unit: storage[1]
          });

        }

      });

    }

  });

  Controller.reopenClass({
    type: 'Resources',
    kind: 'Controller'
  });

  return Controller;

});