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

function EntityGraphLink (scope, element) {
  'ngInject';

  let margin = {
    top: 20,
    right: 30,
    bottom: 40,
    left: 150
  };

  let container = d3.select(element[0].parentNode).node().getBoundingClientRect();

  let width = container.width - margin.left - margin.right;
  let height = 500 - margin.top - margin.bottom;

  let y = d3.scale.ordinal()
    .rangeRoundBands([0, height], 0.1);

  let x = d3.scale.linear()
    .range([0, width]);

  let yAxis = d3.svg.axis()
    .scale(y)
    .orient('left')
    .ticks(5);

  let xAxis = d3.svg.axis()
    .scale(x)
    .orient('bottom')
    .innerTickSize(-height)
    .outerTickSize(0)
    .tickPadding(10);

  let svg = d3.select(element[0]).select('.top-entity-container')
    .append('svg')
      .attr('width', width + margin.left + margin.right)
      .attr('height', height + margin.top + margin.bottom)
    .append('g')
      .attr('transform', 'translate(' + margin.left + ', ' + margin.top + ')');


  /* CREATE GRAPH */
  y.domain(scope.model.results.map((d) => { return d.label; }));
  x.domain(d3.extent(scope.model.results, (d) => { return d.value; })).nice();

  /* AXIS */
  svg.append('g')
    .attr('class', 'x axis')
    .attr('transform', 'translate(0, ' + height + ')')
    .call(xAxis);

  svg.append('g')
    .attr('class', 'y axis')
    .call(yAxis);


  svg.selectAll('.bar')
      .data(scope.model.results)
    .enter().append('rect')
      .attr('class', 'bar')
      .attr('y', (d) => { return y(d.label); })
      .attr('height', y.rangeBand())
      .attr('x', 0)
      .attr('width', (d) => {
        return Math.abs(x(d.value));
      });

  svg.insert("g", ".bars")
     .attr("class", "grid vertical")
     .attr("transform", "translate(0," + (height-margin.top-margin.bottom)  + ")")
     .call(d3.svg.axis().scale(x)
         .orient("bottom")
         .tickSize(-(height-margin.top-margin.bottom), 0, 0)
         .tickFormat("")
     );

}

angular.module(PKG.name + '.feature.tracker')
  .directive('myTopEntityGraph', function (d3) {
    return {
      restrict: 'E',
      scope: {
        model: '=',
        type: '@'
      },
      templateUrl: '/assets/features/tracker/directives/top-entity-graph/top-entity-graph.html',
      link: EntityGraphLink
    };
  });
