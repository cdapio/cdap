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
    series: Em.ArrayProxy.create({ content: [] }),

    // Holds temporary information from the 'Add Metric' form.
    configuring: Em.Object.extend({
      noElement: function () {
        return this.get('element.name') === 'Elements';
      }.property('element'),
      element: {
        name: 'Elements',
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
          this.set('series.content', JSON.parse(saved).content);

        }

      }

      /*
       * Get all available Elements for selection.
       */
      this.HTTP.rest('all', function (models, status) {

        var i = models.length;
        while (i--) {
          models[i].type = 'App';
          models[i] = C[models[i].type].create(models[i]);
        }

        self.set('elementsList', models);

      });

    },

    unload: function () {

      // Unload

    },

    redraw: function () {

      var self = this;
      var urls = [];

      this.get('series').forEach(function (item) {
        urls.push(item.path + '?count=60');
      });

      this.HTTP.post('metrics', urls, function (response, status) {

        self.set('data', response.result[0].result.data);

      });

    }.observes('series.[]'),

    showConfigure: function (metric) {

      // TODO: Find metric to allowe editing in place.

      var top = $('.analyze-selected-metric-add').position().top;

      $('#analyze-configurator').css({ top: (top + 8) + 'px' });
      $('#analyze-configurator').show();

    },

    hideConfigure: function () {

      $('#analyze-configurator').hide();

    },

    selectElement: function (model) {

      model.type = 'App';
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

      var series = {
        element: element.name,
        metric: metric.name,
        path: path,
        color: color
      };

      this.get('series').pushObject(series);

      this.saveLocal();
      this.hideConfigure();

    },

    removeFromChart: function (series) {

      this.get('series').removeObject(series);
      this.saveLocal();

    },

    /**
     * Saves existing application state to localstorage.
     */
    saveLocal: function() {
      localStorage.setItem(STORED_APP_NAME, JSON.stringify(this.get('series')));
    }

  });

  Controller.reopenClass({
    type: 'Analyze',
    kind: 'Controller'
  });

  return Controller;

});