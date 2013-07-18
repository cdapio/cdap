/*
 * Drop Zone Embeddable
 */

define([
  ], function () {

    var x, y;

    // create a line object that represents the SVN line we're creating
    var line = d3.svg.line()
    // assign the X function to plot our line as we wish
    .x(function(d,i) {
      // verbose logging to show what's actually being done
      //console.log('Plotting X value for data point: ' + d + ' using index: ' + i + ' to be at: ' + x(i) + ' using our xScale.');
      // return the X coordinate where we want to plot this datapoint
      return x(i);
    })
    .y(function(d) {
      // verbose logging to show what's actually being done
      //console.log('Plotting Y value for data point: ' + d + ' to be at: ' + y(d) + " using our yScale.");
      // return the Y coordinate where we want to plot this datapoint
      return y(d.value);
    })
    .interpolate("basis");

  var Embeddable = Em.View.extend({
    classNames: ['dash-chart'],

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

            this.get('container').html('');
            this.get('container').css({margin: ''});

            var widget = d3.select(this.get('container')[0]);
            var sparkline = C.Util.sparkline(widget, [],
              this.get('width'), this.get('height'), true);

            this.set('sparkline', sparkline);

          }
        }

        this.get('sparkline').update('A', data);

      }

    },

    fillContainer: function (rerender) {

      var width = $(this.get('container')).outerWidth();
      var height = this.get('height') || $(this.get('container')).outerHeight();

      width += 64;

      this.set('width', width);
      this.set('height', height);

      if (rerender) {
        this.render(true);
      }

    },

    didInsertElement: function () {

      var kind = this.get('kind');
      var w = this.get('width') || $(this.get('element')).outerWidth();
      var h = this.get('height') || $(this.get('element')).outerHeight();

      var container = $('<div class="dash-chart-container"></div>');
      this.set('container', container);
      $(this.get('element')).append(container);

      this.set('width', w);
      this.set('height', h);

      this.addObserver('controller.timeseries.' + kind, this, this.render);

      var self = this;

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
    type: 'DashChart',
    kind: 'Embeddable'
  });

  return Embeddable;

});