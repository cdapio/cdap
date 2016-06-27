/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

angular.module(PKG.name + '.feature.tracker')
  .directive('myHistogram', function (d3) {

    function HistogramLink (scope, element) {
      let margin = {
        top: 20,
        right: 10,
        bottom: 30,
        left: 45
      };

      let parentHeight = 200;

      let container = d3.select(element[0].parentNode).node().getBoundingClientRect();
      let width = container.width - margin.left - margin.right;
      let height = parentHeight - margin.top - margin.bottom;


      /* FORMATTING TIME */
      let data = scope.model.results.map((d) => {
        return {
          time: new Date(d.timestamp * 1000),
          count: d['value']
        };
      });


      let x = d3.time.scale()
        .range([
          (width * 0.2 / data.length / 2) + (width / data.length),
          width - (width / data.length / 2)
        ]);

      let y = d3.scale.linear()
        .range([height, 0]);

      let parentContainer = d3.select(element[0]).select('.histogram-container')
        .style({
          height: parentHeight + 'px'
        });

      let svg = parentContainer.append('svg')
          .attr('width', width + margin.left + margin.right)
          .attr('height', parentHeight)
        .append('g')
          .attr('transform', 'translate(' + margin.left + ', ' + margin.top + ')');


      /* CREATE GRAPH */
      x.domain([data[0].time, data[data.length - 1].time]);
      y.domain([0, d3.max(data, (d) => { return d.count; })]).nice();

      /* X AXIS */
      let timeFormat = d3.time.format('%b %d, %Y');
      let xAxis = d3.svg.axis()
        .scale(x)
        .orient('bottom')
        .tickPadding(10)
        .tickFormat(timeFormat)
        .outerTickSize(0)
        .innerTickSize(0)
        .ticks(d3.time.week, 1);

      svg.append('g')
        .attr('class', 'x axis')
        .attr('transform', 'translate(0, ' + height + ')')
        .call(xAxis);


      /* Y AXIS */
      let yAxis = d3.svg.axis()
        .scale(y)
        .orient('left')
        .tickPadding(10)
        .outerTickSize(0)
        .innerTickSize(-width)
        .ticks(5);

      svg.append('g')
        .attr('class', 'y axis')
        .call(yAxis);


      /* CREATING TOOLTIP */
      let tip = d3.tip()
        .attr('class', 'd3-tip tracker-audit-histogram')
        .offset([-10, 0])
        .html(function(d) {
          return '<strong>' + timeFormat(d.time) + ' - ' + d.count +  '</strong>';
        });
      svg.call(tip);

      /* BARS */
      svg.selectAll('.bar')
          .data(data)
        .enter().append('rect')
          .attr('class', 'bar')
          .attr('x', (d) => { return x(d.time) - (width / data.length); })
          .attr('y', (d) => { return y(d.count); })
          .attr('width', width/data.length - (width * 0.2 / data.length ))
          .attr('height', (d) => { return height - y(d.count); })
        .on('mouseover', tip.show)
        .on('mouseout', tip.hide);

    }


    return {
      restrict: 'E',
      scope: {
        model: '='
      },
      templateUrl: '/assets/features/tracker/directives/histogram/histogram.html',
      link: HistogramLink
    };
  });
