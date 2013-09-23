/*
 * Resources Controller
 */

define([], function () {

  var DASH_CHART_COUNT = 60;

  var Controller = Em.Controller.extend({

    structure: { text: 'Root', children: Em.ArrayProxy.create({ content: [] }) },
    elements: Em.Object.create(),

    __remaining: -1,

    currents: Em.Object.create(),
    timeseries: Em.Object.create(),
    value: Em.Object.create(),

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
      this.set('elements.Batch', Em.ArrayProxy.create({ content: [] }));
      this.set('elements.Workflow', Em.ArrayProxy.create({ content: [] }));
      this.set('elements.Procedure', Em.ArrayProxy.create({ content: [] }));

      this.HTTP.rest('apps', function (objects) {

        var structure = self.get('structure');
        var elements = self.get('elements');

        var pending = 0;

        function next (object) {

          object.children = object.children || Em.ArrayProxy.create({ content: [] });

          if (typeof object.getSubPrograms === 'function') {

            pending ++;

            object.getSubPrograms(function (programs) {

              pending --;

              for (var type in programs) {

                // Push list into controller cache.
                elements[type].pushObjects(programs[type]);

                var i = programs[type].length, program, context;
                while (i--) {

                  program = programs[type][i];
                  context = '/reactor' + program.get('context') + '/';

                  program.trackMetric(context + 'resources.used.memory', 'timeseries');
                  program.trackMetric(context + 'resources.used.containers', 'currents', 'containers');
                  program.trackMetric(context + 'resources.used.vcores', 'currents', 'cores');

                  program.set('pleaseObserve', context + 'resources.used.memory');

                  object.children.pushObject(program);
                  next(program);

                }

              }

            }, self.HTTP);

          }

        }

        var i = objects.length;
        while (i--) {
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

      /*
       * Check disk space
       */
      if (C.Env.cluster) {

        this.HTTP.get('disk', function (disk) {
          if (disk) {
            var bytes = C.Util.bytes(disk.free);
            $('#diskspace').find('.sparkline-box-title').html(
              'Storage (' + bytes[0] + bytes[1] + ' Free)');
          }
        });

      }

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

      var self = this, types = ['App', 'Flow', 'Flowlet', 'Batch', 'Workflow', 'Procedure'];

      var i, models = [];
      for (i = 0; i < types.length; i ++) {
        models = models.concat(this.get('elements').get(types[i]).get('content'));
      }

      var now = new Date().getTime();

      // Add a two second buffer to make sure we have a full response.
      var start = now - ((C.__timeRange + 2) * 1000);
      start = Math.floor(start / 1000);

      this.clearTriggers(false);

      // Scans models for timeseries metrics and updates them.
      C.Util.updateTimeSeries(models, this.HTTP, this);

      // Scans models for current metrics and udpates them.
      C.Util.updateCurrents(models, this.HTTP, this);

      // Hax. Count is timerange because server treats end = start + count (no downsample yet)
      var queries = [
        '/reactor/resources.used.memory?count=' + C.__timeRange + '&start=' + start,
        '/reactor/resources.used.containers?count=' + C.__timeRange + '&start=' + start,
        '/reactor/resources.used.vcores?count=' + C.__timeRange + '&start=' + start
      ], self = this;

      function lastValue(arr) {
        return arr[arr.length - 1].value;
      }

      this.HTTP.post('metrics', queries, function (response) {
        self.set('miscCompleted', true);
        if (response.result) {

          var result = response.result;

          self.set('timeseries.memory', result[0].result.data);
          self.set('timeseries.containers', result[1].result.data);
          self.set('timeseries.cores', result[2].result.data);

          var memory = C.Util.bytes(lastValue(result[0].result.data));
          self.set('value.memory', {
            label: memory[0],
            unit: memory[1]
          });

          self.set('value.containers', lastValue(result[1].result.data));
          self.set('value.cores', lastValue(result[2].result.data));

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