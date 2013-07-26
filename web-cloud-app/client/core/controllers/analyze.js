/*
 * Analyze Controller.
 * This manages all actions executed from metrics explorer.
 */

define(['../../helpers/chart-helper'], function (chartHelper) {

  // Used for local storage, which is used to store our metrics selection.
  var STORED_APP_NAME = 'continuuity-analyze';
  var DEFAULT_COLOR = 'orange';

  var Controller = Em.Controller.extend(Em.Evented, {

    // Holds available element models.
    elementsList: [],

    // Holds available metrics.
    metricsList: [],

    // Holds a per-type cache of available metrics.
    metricsCache: Em.Object.create(),

    // Holds the currently selected metrics.
    selected: Em.ArrayProxy.create({ content: [] }),

    // Holds temporary information from the 'Add Metric' form.
    configuring: Em.Object.extend({
      noSelection: function () {
        return this.get('element.name') === 'Elements';
      }.property('element'),
      element: {
        name: 'Elements'
      },
      metric: {
        name: 'Metric Names'
      },
      color: DEFAULT_COLOR
    }),

    init: function () {

      /*
       * Only 'extend' allows computed properties, as above.
       * Therefore we create / construct here.
       */
      this.set('configuring', this.get('configuring').create());

    },

    load: function (id) {

      var self = this;

      this.set('selected', Em.ArrayProxy.create({ content: [] }));

      /*
       * Prepopulate metrics selection from Local Storage if available.
       */
      if (window.localStorage) {

        var saved = localStorage.getItem(STORED_APP_NAME);
        if (saved) {
          this.set('selected.content', JSON.parse(saved).content);

        }

      }

      /*
       * Get all available Elements for selection.
       */
      this.HTTP.rest('all', function (models, status) {

        var i = models.length, names = [];
        while (i--) {
          if (C[models[i].type]) {
            models[i] = C[models[i].type].create(models[i]);
          }
        }

        var map = {};
        $.each(models, function (i, element) {
          var label = element.name + ' (' + element.type + ')';
          map[label] = element;
          names.push(label);
        });

        $('#elementSelector').typeahead({
          source: names,
          updater: function (item) {

            var element = map[item];
            self.selectElement(element);

          }
        });

      });

    },

    unload: function () {

      // Unload

    },

    colorOptions: [
      { name: 'Orange', code: 'orange' },
      { name: 'Blue', code: 'blue' },
      { name: 'Red', code: 'red' }],

    update: function () {

      var self = this;
      var urls = [];

      var selected = this.get('selected');
      var start, now = new Date().getTime();

      // Add a two second buffer to make sure we have a full response.
      start = now - ((C.__timeRange + 2) * 1000);
      start = Math.floor(start / 1000);

      this.get('selected').forEach(function (item) {
        urls.push(item.path + '?start=' + start + '&count=60');
      });

      function findMetric (path) {
        var i = selected.content.length;
        while (i --) {
          if (path.indexOf(selected.content[i].path) === 0) {
            return selected.content[i];
          }
        }
      }

      this.HTTP.post('metrics', urls, function (response, status) {

        if (response.result) {

          var s = null, series = [], selected = self.get('selected.content'),
            result = response.result;

          for (var i = 0; i < result.length; i ++) {

            if ((metric = findMetric(result[i].path))) {

              s = {
                name: metric.path,
                color: metric.color,
                data: []
              }, d = response.result[i].result.data;

              for (var j = 0; j < d.length; j ++) {
                s.data.push({
                  x: d[j].time * 1000,
                  y: d[j].value
                });
              }

              series.push(s);

            }

          }
          self.set('series', series);

        }

        setTimeout(function () {
          self.update();
        }, 1000);

      });

    }.observes('selected.[]'),

    showConfigure: function (metric) {

      // TODO: Find metric to allow editing in place.
      var top = $('.analyze-selected-metric-add').position().top;

      $('#analyze-configurator').css({ top: (top + 8) + 'px' });
      $('#analyze-configurator').show();

    },

    hideConfigure: function () {

      $('#analyze-configurator').hide();

    },

    selectColor: function (color) {

      this.configuring.set('color', color.code);

    },

    selectElement: function (model) {

      var self = this;

      this.configuring.set('element', model);

      if (this.get('metricsCache').get(model.type)) {
        this.set('metricsList', this.get('metricsCache').get(model.type));

      } else {

        this.HTTP.rest('metrics', model.type, function (metrics, status) {

          self.get('metricsCache').set(model.type, metrics);
          self.set('metricsList', metrics);

        });

      }

    },

    selectMetricName: function (metric) {

      this.configuring.set('metric', metric);

    },

    addToChart: function () {

      var element = this.get('configuring.element');
      var metric = this.get('configuring.metric');
      var color = this.get('configuring.color');

      var path = element.interpolate(metric.path);

      var selected = {
        element: element.name,
        metric: metric.name,
        path: path,
        color: color
      };

      this.get('selected').pushObject(selected);

      this.saveLocal();
      this.hideConfigure();

    },

    removeFromChart: function (selected) {

      this.get('selected').removeObject(selected);
      this.saveLocal();

    },

    /**
     * Saves existing application state to localstorage.
     */
    saveLocal: function() {
      localStorage.setItem(STORED_APP_NAME, JSON.stringify(this.get('selected')));
    }

  });

  Controller.reopenClass({
    type: 'Analyze',
    kind: 'Controller'
  });

  return Controller;

});