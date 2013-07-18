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

    build: function (width, height, data) {

      var graph = d3.select(this.get('element')).append("svg:svg").attr("width", "100%").attr("height", "100%");

      this.set('built', true);

      return graph;

    },

    render: function () {

      var kind = this.get('controller.timeseries.' + this.get('kind'));
      var width = this.get('width');
      var height = this.get('height');

      if (!this.get('built')) {
        this.set('graph', this.build(width, height));
      }

      // X scale will fit values from 0-10 within pixels 0-100
      x = d3.scale.linear().domain([0, 48]).range([-5, width]); // starting point is -5 so the first value doesn't show and slides off the edge as part of the transition
      // Y scale will fit values from 0-10 within pixels 0-100
      y = d3.scale.linear().domain([0, 10]).range([0, height]);


      var graph = this.get('graph');

      if (kind) {

        var data = kind.slice(0);

        graph.selectAll("path")
          .data([data]) // set the new data
          .attr("transform", "translate(" + x(1) + ")") // set the transform to the right by x(1) pixels (6 for the scale we've set) to hide the new value
          .attr("d", line) // apply the new data values ... but the new value is hidden at this point off the right of the canvas
          .transition() // start a transition to bring the new value into view
          .ease("linear")
          .duration(1000) // for this demo we want a continual slide so set this to the same as the setInterval amount below
          .attr("transform", "translate(" + x(0) + ")"); // animate a slide to the left back to x(0) pixels to reveal the new value
      }

    },

    didInsertElement: function () {

      var kind = this.get('kind');
      var w = this.get('width') || $(this.get('element')).outerWidth();
      var h = this.get('height') || $(this.get('element')).outerHeight();

      this.set('width', w);
      this.set('height', h);

      // this.addObserver('controller.timeseries.' + kind, this, this.render);

    }
  });

  Embeddable.reopenClass({
    type: 'DashChart',
    kind: 'Embeddable'
  });

  return Embeddable;

});