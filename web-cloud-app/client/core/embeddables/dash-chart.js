/*
 * Drop Zone Embeddable
 */

define([
  ], function () {

  var Embeddable = Em.View.extend({
    classNames: ['dash-chart'],

    build: function (w, h, data) {

      var x = d3.scale.linear()
        .domain([0, 1])
        .range([0, w / data.length]);
      var y = d3.scale.linear()
        .domain([0, 100])
        .rangeRound([0, h]);

      this.set('x', x);
      this.set('y', y);

      var chart = d3.select(this.get('element')).append("svg")
        .attr("class", "chart")
        .attr("width", w)
        .attr("height", h);

      /*
      chart.append("line")
        .attr("x1", 0)
        .attr("x2", w)
        .attr("y1", h - .5)
        .attr("y2", h - .5)
        .style("stroke", "#000");
      */

      w = w / data.length;

      chart.selectAll("rect")
        .data(data)
        .enter().append("rect")
          .attr("x", function(d, i) { return x(i) - .5; })
          .attr("y", function(d) { return h - y(d.value) - .5; })
          .attr("width", w)
          .attr("height", function(d) { return y(d.value); });

      return chart;

    },

    render: function () {

      var kind = this.get('controller.timeseries.' + this.get('kind'));

      if (kind) {

        var data = kind.slice(0);

        var w = this.get('width');
        var h = this.get('height');

        var chart = this.get('chart');
        if (!chart) {
          this.set('chart', chart = this.build(w, h, data));
        }

        var x = this.get('x');
        var y = this.get('y');

        chart.selectAll("rect")
          .data(data)
          .transition()
          .duration(1000)
          .attr("y", function(d) { return h - y(d.value) - .5; })
          .attr("height", function(d) { return y(d.value); });

        return;

        var rect = chart.selectAll("rect")
          .data(data, function(d) { return d.timestamp; });


        w = Math.floor(w / data.length);

        rect.enter().insert("rect", "line")
          .attr("x", function(d, i) { return x(i + 1) - .5; })
          .attr("y", function(d) { return h - y(d.value) - .5; })
          .attr("width", w)
          .attr("height", function(d) { return y(d.value); })
        .transition().duration(1000)
          .attr("x", function(d, i) { return x(i) - .5; });

        rect.transition()
          .duration(1000)
          .attr("x", function(d, i) { return x(i) - .5; });

        rect.exit().transition()
          .duration(1000)
          .attr("x", function(d, i) { return x(i - 1) - .5; })
          .remove();

      }

    },

    didInsertElement: function () {

      var kind = this.get('kind');
      var w = this.get('width') || $(this.get('element')).outerWidth();
      var h = this.get('height') || $(this.get('element')).outerHeight();

      this.set('width', w);
      this.set('height', h);

      this.addObserver('controller.timeseries.' + kind, this, this.render);

    }
  });

  Embeddable.reopenClass({
    type: 'DashChart',
    kind: 'Embeddable'
  });

  return Embeddable;

});