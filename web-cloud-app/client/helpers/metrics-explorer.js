/*
 * Metrics explorer helper. This contains the chart libaries used for rendering metrics explorer
 * data.
 */

define(['d3', 'nvd3'], function () {

  var chartHelper = {};

  chartHelper.Chart = function(data, divId) {
    console.log('got here');
    console.log(data);

    var margin = {top: 20, right: 80, bottom: 30, left: 80},
    width = 960 - margin.left - margin.right,
    height = 500 - margin.top - margin.bottom;

    var parseDate = d3.time.format("%Y%m%d").parse;

    var x = d3.time.scale()
        .range([0, width]);

    var y = d3.scale.linear()
        .range([height, 0]);

    var color = d3.scale.category10();

    var xAxis = d3.svg.axis()
        .scale(x)
        .orient("bottom")
        .ticks(10);

    var yAxis = d3.svg.axis()
        .scale(y)
        .orient("left")
        .ticks(10);

    var line = d3.svg.line()
        .interpolate("cardinal")
        .x(function(d) { return x(d.timestamp); })
        .y(function(d) { return y(d.value); });

    var svg = d3.select('#'+divId).append("svg") 
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
      .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    color.domain(d3.keys(data[0]).filter(function(key) { return key !== "timestamp"; }));

    var cities = color.domain().map(function(name) {
      return {
        name: name,
        values: data.map(function(d) {
          return {timestamp: d.timestamp, value: +d[name]};
        })
      };
    });

    x.domain(d3.extent(data, function(d) { return d.timestamp; }));

    y.domain([
        d3.min(cities, function(c) { return d3.min(c.values, function(v){ return v.value }); }),
        d3.max(cities, function(c) { return d3.max(c.values, function(v){ return v.value }); })
    ]);

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis)
      .append("text")
        .attr("x", width)
        .attr("y", -6)
        .style("text-anchor", "end")
        .text("Time");

    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis)
      .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", 6)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text("Events");

    svg.append("g")         
        .attr("class", "grid")
        .style("stroke-dasharray", ("3, 3"))
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis
            .tickSize(-height, 0, 0)
            .tickFormat("")
        )

    svg.append("g")         
        .attr("class", "grid")
        .style("stroke-dasharray", ("3, 3"))
        .call(yAxis
            .tickSize(-width, 0, 0)
            .tickFormat("")
        )

    var city = svg.selectAll(".city")
        .data(cities)
        .enter().append("g")
        .attr("class", "city");

    city.append("path")
        .attr("class", "line")
        .attr("d", function(d) { return line(d.values); })
        .style("stroke", function(d) { return color(d.name); });

    city.append("text")
        .datum(function(d) { return {name: d.name, value: d.values[d.values.length - 1]}; })
        .attr("transform", function(d) { return "translate(" + x(d.value.timestamp) + "," + y(d.value.value) + ")"; })
        .attr("x", 3)
        .attr("dy", ".8em")
        .text(function(d) { return d.name; });

    color.domain().map(function(name) {
        svg.selectAll("dot")
           .data(data)
           .enter().append("circle")
           .attr("r", 3.5)
           .attr("cx", function(d) { return x(d.timestamp); })
           .attr("cy", function(d) { return y(d[name]); });
    });


  };

  return chartHelper;

});
