/*
 * Analyze Controller.
 * This manages all actions executed from metrics explorer.
 */

define(['../../helpers/chart-helper'], function (chartHelper) {

  // Used for local storage, which is used to store our metrics selection.
  var STORED_APP_NAME = 'continuuity-analyze';
  var DEFAULT_COLOR = 'orange';

  var Controller = Em.Controller.extend(Em.Evented, {

    elementModels: [],

    // Holds available element models.
    elementsCache: [],

    // Holds a per-type cache of available metrics.
    metricsCache: Em.Object.create(),

    // Holds the currently selected metrics.
    selected: Em.ArrayProxy.create({ content: [] }),

    // Holds temporary information from the 'Add Metric' form.
    configuring: Em.Object.extend({
      noSelection: function () {
        return this.get('element.name') === 'Elements' ||
          this.get('metric.name') === 'Metric Names';
      }.property('element', 'metric'),
      element: {
        name: 'Elements'
      },
      metric: {
        name: 'Metric Names'
      },
      color: DEFAULT_COLOR,
      colorStyle: function () {
        return 'background-color: ' + this.get('color');
      }.property('color')
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

      function findElement(type, id) {

        var models = self.elementModels, i = models.length;
        while (i--) {
          if (models[i].type === type && models[i].id === id) {
            return models[i];
          }
        }
        return null;

      }

      // Move to Ember run loop
      setTimeout(function () {

        $("#elementSelector").select2({
            containerCssClass: "analyze-configurator-element-select",
            placeholder: "Select Element",
            query: function(query) {

              var data = [];

              $.each(self.elementsCache, function(){

                var children = [];
                $.each(this.children, function () {
                  if(query.term.length == 0 || this.text.toUpperCase().indexOf(query.term.toUpperCase()) >= 0 ){
                      children.push({id: this.id, text: this.text });
                  }
                });
                this.children = children;
                if (children.length) {
                  data.push(this);
                }
              });
              query.callback({results: data});

            },
        });
        $("#elementSelector").on('change', function (e) {

          if (e.added) {
            var id = e.added.id.split('|');
            var element = findElement(id[0], id[1]);
            self.selectElement(element);
          }

        });

        function filterMetrics(term, metrics) {

          var results = [];
          $.each(metrics, function () {
            if(term.length == 0 || this.text.toUpperCase().indexOf(term.toUpperCase()) >= 0 ){
                results.push({id: this.id, text: this.text });
            }
          });
          return results;

        }

        $("#metricSelector").select2({
            placeholder: "Select Metric",
            query: function(query) {

              var model = self.configuring.element;
              var metrics = self.metricsCache.get(model.type);

              if (metrics) {

                query.callback({results: filterMetrics(query.term, metrics)});

              } else {

                self.HTTP.rest('metrics', model.type, function (metrics, status) {

                  var i = metrics.length;
                  while (i--) {
                    metrics[i] = {
                      id: metrics[i].path,
                      text: metrics[i].name
                    };
                  }

                  self.get('metricsCache').set(model.type, metrics);
                  self.set('metricsList', metrics);

                  query.callback({results: filterMetrics(query.term, metrics)});

                });

              }

            }

        });
        $("#metricSelector").on('change', function (e) {

          if (e.added) {
            self.selectMetric({
              name: e.added.text,
              path: e.added.id
            });
          }

        });

      }, 500);

      function sortByKey (obj) {

        var keys = [];
        var sorted_obj = {};

        for(var key in obj){
            if(obj.hasOwnProperty(key)){
                keys.push(key);
            }
        }
        keys.sort();
        jQuery.each(keys, function(i, key){
            sorted_obj[key] = obj[key];
        });

        return sorted_obj;
      };

      /*
       * Get all available Elements for selection.
       */
      this.HTTP.rest('all', function (models, status) {

        var i = models.length;
        while (i--) {
          if (C[models[i].type]) {
            models[i] = C[models[i].type].create(models[i]);
            self.elementModels.push(models[i]);
          }
        }

        var byType = {};

        $.each(models, function (i, element) {

          var id = element.type + '|' + element.id;

          if (byType[element.type]) {
            byType[element.type].push({
              id: id,
              text: element.name
            })
          } else {
            byType[element.type] = [{
              id: id,
              text: element.name
            }];
          }

        });

        byType = sortByKey(byType);

        for (var type in byType) {
          self.elementsCache.push({
            text: type + 's',
            children: byType[type]
          });
        }

      });

    },

    unload: function () {

      this.set('elementModels', []);
      this.set('elementsCache', []);
      this.set('metricsCache', Em.Object.create());

    },

    colors: ['#fce94f', '#edd400', '#c4a000', '#fcaf3e', '#f57900', '#ce5c00',
    '#e9b96e', '#c17d11', '#8f5902', '#8ae234', '#73d216', '#4e9a06', '#729fcf',
    '#3465a4', '#204a87', '#ad7fa8', '#75507b', '#5c3566', '#ef2929', '#cc0000'],

    update: function () {

      var self = this;
      var urls = [];

      var selected = this.get('selected');
      var start, now = new Date().getTime();

      // Add a two second buffer to make sure we have a full response.
      start = now - ((C.__timeRange + 2) * 1000);
      start = Math.floor(start / 1000);

      this.get('selected').forEach(function (item) {
        urls.push(item.path + '?start=' + start + '&count=' + C.__timeRange);
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
                name: metric.element + ' (' + metric.type + ') ' + metric.metric + ')',
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
      var index = Math.floor(Math.random() * this.colors.length);

      this.configuring.set('color', this.colors[index]);
      this.configuring.set('element', { name: 'Elements' });
      this.configuring.set('metric', { name: 'Metric Names' });

      $("#elementSelector").select2('val', null);
      $("#metricSelector").select2('val', null);

      $('#analyze-configurator').css({ top: (top + 8) + 'px' });
      $('#analyze-configurator').fadeIn(100);

    },

    hideConfigure: function () {

      $('#analyze-configurator').fadeOut(100);

    },

    selectColor: function (color) {

      this.configuring.set('color', color);

    },

    selectElement: function (model) {

      this.configuring.set('element', model);
      $("#metricSelector").select2("enable", true);

    },

    selectMetric: function (metric) {

      this.configuring.set('metric', metric);

    },

    addToChart: function () {

      var element = this.get('configuring.element');
      var metric = this.get('configuring.metric');
      var color = this.get('configuring.color');

      var path = element.interpolate(metric.path);
      var selected = {
        element: element.name,
        type: element.type,
        metric: metric.name,
        path: path,
        color: color,
        href: element.get('href')
      };

      this.get('selected').pushObject(selected);

      this.saveLocal();
      this.hideConfigure();

    },

    removeFromChart: function (selected) {

      this.get('selected').removeObject(selected);
      this.saveLocal();
      this.hideConfigure();

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