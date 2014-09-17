/*
 * Metrics explorer helper. This contains the chart libaries used for rendering metrics explorer and
 * helpers for manipulating metric data.
 */

define([], function () {

  // Set up chart helper namespace.
  var chartHelper = {};

  /**
   * Constructor for metrics explorer chart. Renders D3 chart.
   * @param {Object} data chart data.
   * @param {Array} metrics list of metrics to render.
   * @param {string} divId id of div to insert chart into.
   */
  chartHelper.ChartHelper = function(data, metrics, divId, width) {

    this.metrics = metrics;

    var self = this;

    // Boilerplate D3.
    var margin = {top: 20, right: 10, bottom: 30, left: 10},
    width = width - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

    var x = d3.time
              .scale()
              .range([0, width]);

    var y = d3.scale
              .linear()
              .range([height, 0]);

    var color = d3.scale.category10();

    var xAxis = d3.svg
                  .axis()
                  .scale(x)
                  .orient("bottom")
                  .ticks(10);

    var yAxis = d3.svg
                  .axis()
                  .scale(y)
                  .orient("left")
                  .ticks(10);

    var line = d3.svg
                 .line()
                 .interpolate("cardinal")
                 .x(function(d) { return x(d.timestamp); })
                 .y(function(d) { return y(d.value); });

    // Append new svg to specified div.
    var svg = d3.select('#'+divId)
                .append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    // Get list of existing metrics and add to color domain. Later used for generating colors.
    color.domain(d3.keys(data[0]).filter(function(key) { return key !== "timestamp"; }));

    // Format chart data for drawing.
    var datasets = color.domain().map(function(name) {
      return {
        name: name,
        values: data.map(function(d) {
          return {timestamp: d.timestamp, value: +d[name]};
        })
      };
    });

    var metric = svg.selectAll(".metric")
                    .data(datasets)
                    .enter().append("g")
                    .attr("class", "metric");

    var div = d3.select('body')
                .append("div")
                .attr("class", "tooltip")
                .style("opacity", 0);

    // Sets limits for x axis.
    x.domain(d3.extent(data, function(d) { return d.timestamp; }));

    // Sets limits for y-axis.
    y.domain([
        d3.min(datasets, function(c) { return d3.min(c.values, function(v){ return v.value }); }),
        d3.max(datasets, function(c) { return d3.max(c.values, function(v){ return v.value }); })
    ]);

    // Draw x axis.
    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis)
        .append("text")
        .attr("x", width)
        .attr("y", -6)
        .style("text-anchor", "end")
        .text("Time");

    // Draw y axis.
    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis)
        .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", 6)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text("Events");

    // Draw background dotted grid on x axis.
    svg.append("g")
        .attr("class", "grid")
        .style("stroke-dasharray", ("3, 3"))
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis
            .tickSize(-height, 0, 0)
            .tickFormat("")
        )

    // Draw background dotted grid on y axis.
    svg.append("g")
        .attr("class", "grid")
        .style("stroke-dasharray", ("3, 3"))
        .call(yAxis
            .tickSize(-width, 0, 0)
            .tickFormat("")
        )

    // Draw graph lines.
    metric.append("path")
        .attr("class", "line")
        .attr("d", function(d) { return line(d.values); })
        .style("stroke", function(d) {
          return 'green'; //self.getColorForMetric(d.name) || color(d.name);
        })
        .style("stroke-width", '3');

    // Set labels for each graph line.
    // metric.append("text")
    //     .datum(function(d) { return {name: d.name, value: d.values[d.values.length - 1]}; })
    //     .attr("transform", function(d) {
    //       return "translate(" + x(d.value.timestamp) + "," + y(d.value.value) + ")"; })
    //     .attr("x", 3)
    //     .attr("dy", ".8em")
    //     .text(function(d) { return d.name; });

    // Draw dots on each graph line and generate tooltip on mouseover.
    color.domain().map(function(name) {
        svg.selectAll("dot")
           .data(data)
           .enter().append("circle")
           .attr("r", 4)
           .attr("cx", function(d) { return x(d.timestamp); })
           .attr("cy", function(d) { return y(d[name]); })
           .on("mouseover", function(d) {
              d3.select(this)
                .attr("r", d3.select(this).attr("r") * 1 + 5 );
              div.transition()
                 .duration(200)
                 .style("opacity", .9);
              div .html('<b>'+chartHelper.reformTerm(name) + '</b><br />' + d[name])
                  .style("left", (d3.event.pageX) + "px")
                  .style("top", (d3.event.pageY - 28) + "px");
            })
          .on("mouseout", function(d) {
            d3.select(this)
              .transition()
              .duration(500)
              .attr("r", 4);
            div.transition()
               .duration(500)
               .style("opacity", 0);
        });
    });
  };

  /**
   * Gets color from a hash of metric data based on metric name for display purposes.
   * @param {string} name app metric token eg: twitter_scanner_events_in.
   * @return {string|null} color if color exists or null.
   */
  chartHelper.ChartHelper.prototype.getColorForMetric = function(name) {

    for (metric in this.metrics) {
      if (this.metrics.hasOwnProperty(metric)) {
        var modifiedName = chartHelper.getAppName(
          chartHelper.urlRestify(this.metrics[metric]['app']),
          chartHelper.urlRestify(this.metrics[metric]['name']));
        if (modifiedName === name) {
          return this.metrics[metric]['color'];
        }
      }
    }
    return null;

  };

  return chartHelper;

});
