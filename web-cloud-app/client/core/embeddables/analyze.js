/*
 * Analyze tab Embeddable. This manages the graph on the Analyze tab.
 * Depends on Analyze controller and must be embedded inside of it.
 */

define(['../../helpers/chart-helper'], function (chartHelper) {

    var seriesOptions = {
      marker: {
        enabled: false,
        states: {
          hover: {
            enabled: false
          }
        }
      }
    };

    var Embeddable = Em.View.extend({

      elementId: 'metrics-explorer-widget',

      build: function (series) {

        var graph = new Rickshaw.Graph( {
          element: this.get('element'),
          width: this.$().width(),
          height: 500,
          renderer: 'line',
          series: series,
          interpolation: 'linear',
          padding: {
            top: 0.2
          }
        });

        var hoverDetail = new Rickshaw.Graph.HoverDetail( {
          graph: graph,
          xFormatter: function (x) {
            return new Date( x * 1000 ).toString();
          }
        } );

        var axes = new Rickshaw.Graph.Axis.Time( {
          graph: graph,
          timeFixture: new Rickshaw.Fixtures.Time.Local()
        });
        axes.render();

        var y_ticks = new Rickshaw.Graph.Axis.Y( {
          graph: graph,
          orientation: 'left',
          tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
          element: document.getElementById('y_axis')
        } );

        this.set('chart', graph);

      },

      update: function () {

        if (!this.get('controller.series').length) {
          return;
        }

        if (!this.get('chart')) {
            this.build(this.get('controller.series'));
        }

        function diff (current, more) {

          if (!current.length) {
            return more;
          }

          var x = current[current.length - 1].x, index = 0;
          var i = more.length;
          while (i--) {
            if (more[i].x <= x) {
              index = i;
              break;
            }
          }
          return more.slice(index + 1);

        }

        var current = this.get('chart.series');
        var more = this.get('controller.series');

        var i = more.length, j, k, found;
        while (i--) {

          j = current.length, found = false;
          while (j--) {
            if (current[j].name === more[i].name) {
              current[j].data = more[i].data;
              found = true;
            }
          }

          if (!found) {
            $.extend(more[i], seriesOptions);

            this.get('chart').series.addObject(
              more[i]
            );

          }

        }

        i = current.length;
        while (i--) {

          j = more.length, found = false;
          while (j--) {
            if (current[i].name === more[j].name) {
              found = true;
            }
          }
          if (!found) {
            this.get('chart').series.removeAt(i);
          }
        }

        this.get('chart').render();

      }.observes('controller.series'),

      didInsertElement: function () {

        var self = this;

        C.addResizeHandler('metrics-explorer', function () {

          $('#y_axis').html('');
          self.$().html('');

          self.set('chart', null);
          self.update();

        });

      }

    });

    Embeddable.reopenClass({
      type: 'Analyze',
      kind: 'Embeddable'
    });

    return Embeddable;

  });