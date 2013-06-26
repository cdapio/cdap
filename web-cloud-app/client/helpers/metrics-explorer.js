/*
 * Metrics explorer helper. This contains the chart libaries used for rendering metrics explorer
 * data.
 */

define(['/core/lib/nvd3/nv.d3.min.js'], function (nv) {

  console.log(nv);
  var ChartHelper = function() {};

  ChartHelper.prototype.drawLineGraph = function() {
    function() {
      var sin = [],
          cos = [];

      for (var i = 0; i < 100; i++) {
        sin.push({x: i, y: Math.sin(i/10)});
        cos.push({x: i, y: .5 * Math.cos(i/10)});
      }

      return [
        {
          values: sin,
          key: 'Sine Wave',
          color: '#ff7f0e'
        },
        {
          values: cos,
          key: 'Cosine Wave',
          color: '#2ca02c'
        }
      ];
    }

    nv.addGraph(function() {
      var chart = nv.models.lineChart();

      chart.xAxis
          .axisLabel('Time (ms)')
          .tickFormat(d3.format(',r'));

      chart.yAxis
          .axisLabel('Voltage (v)')
          .tickFormat(d3.format('.02f'));

      d3.select('#chart svg')
          .datum(data())
        .transition().duration(500)
          .call(chart);

      nv.utils.windowResize(chart.update);

      return chart;
    });
  }

});
