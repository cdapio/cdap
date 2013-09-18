/*
 * Drop Zone Embeddable
 */

define([
  ], function () {

  var Embeddable = Em.View.extend({
    classNames: ['system-chart'],

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

    render: function (redraw) {

      var kind = this.get('controller.timeseries.' + this.get('kind'));

      if (kind) {

        kind = kind.slice(0);

        var data  =[];
        var i = kind.length;
        while (i--) {
          data.unshift(kind[i].value);
        }

        if (data && data.length) {
          if ((typeof redraw === 'boolean' && redraw) || !this.get('sparkline')) {

            if (!this.get('container').html) {
              return;
            }

            this.get('container').html('');
            this.get('container').css({margin: ''});

            var widget = d3.select(this.get('container')[0]);
            var sparkline = C.Util.sparkline(widget, [],
              this.get('width'), this.get('height'), false, true);

            this.set('sparkline', sparkline);

          }
        }

        this.get('sparkline').update('A', data);

      }

    },

    fillContainer: function (rerender) {

      var width = $(this.get('container')).outerWidth();
      var height = this.get('height') || $(this.get('container')).outerHeight();

      width *= 1.1;

      this.set('width', width);
      this.set('height', height);

      if (rerender) {
        this.render(true);
      }

    },

    didInsertElement: function () {

      var kind = this.get('kind');
      var w = this.get('width') || $(this.get('element')).outerWidth();
      var h = 102;

      var container = $('<div class="system-chart-container"></div>');
      this.set('container', container);
      $(this.get('element')).append(container);

      this.set('width', w);
      this.set('height', h);

      this.addObserver('controller.timeseries.' + kind, this, this.render);

      var self = this;

      this.build([{x: 0, y: 0}]);
      return;

      C.addResizeHandler(kind, function () {
        self.fillContainer(true);
      });

      this.fillContainer();

    },
    willDestroyElement: function () {

      var kind = this.get('kind');
      C.removeResizeHandler(kind);

    }
  });

  Embeddable.reopenClass({
    type: 'SystemChart',
    kind: 'Embeddable'
  });

  return Embeddable;

});