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
  .directive('myTopEntityGraph', function (d3, $compile) {

    function EntityGraphLink (scope, element) {
      let margin = {
        top: 20,
        right: 30,
        bottom: 30,
        left: 130
      };

      let parentHeight = 222;

      let container = d3.select(element[0].parentNode).node().getBoundingClientRect();
      let width = container.width - margin.left - margin.right;
      let height = parentHeight - margin.top - margin.bottom;

      let y = d3.scale.ordinal()
        .rangeRoundBands([0, height], 0.3);

      let x = d3.scale.linear()
        .range([0, width]);

      let parentContainer = d3.select(element[0]).select('.top-entity-container')
        .style({
          height: parentHeight + 'px'
        });

      parentContainer.append('div')
        .attr('class', 'sidebar')
        .style({
          width: '130px',
          height: parentHeight + 'px'
        });

      let svg = parentContainer.append('svg')
          .attr('width', width + margin.left + margin.right)
          .attr('height', parentHeight)
        .append('g')
          .attr('transform', 'translate(' + margin.left + ', ' + margin.top + ')');


      /* CREATE GRAPH */
      y.domain(scope.model.results.map((d) => { return d.label; }));
      x.domain([0, d3.max(scope.model.results, (d) => { return d.value; })]).nice();

      /* X AXIS */
      let xAxis = d3.svg.axis()
        .scale(x)
        .orient('bottom')
        .innerTickSize(-height - margin.top)
        .outerTickSize(0)
        .tickPadding(10)
        .ticks(5);

      svg.append('g')
        .attr('class', 'x axis')
        .attr('transform', 'translate(0, ' + height + ')')
        .call(xAxis);

      // Removing first tick
      svg.select('.x.axis .tick')
        .attr('display', 'none');


      /* Y AXIS */
      let yAxis = d3.svg.axis()
        .scale(y)
        .orient('left')
        .ticks(5);

      svg.append('g')
        .attr('class', 'y axis')
        .call(yAxis);

      /* BARS */
      svg.selectAll('.bar')
          .data(scope.model.results)
        .enter().append('rect')
          .attr('class', 'bar')
          .attr('y', (d) => { return y(d.label); })
          .attr('x', -3)
          .attr('rx', 3)
          .attr('ry', 3)
          .attr('height', y.rangeBand())
          .attr('width', (d) => { return Math.abs(x(d.value)) + 3; });

      /* Adding Links */
      let sidebarElem = angular.element(parentContainer[0]).find('div');

      if (scope.type === 'apps') {
        angular.forEach(scope.model.results, (result) => {
          let link = angular.element('<a></a>')
            .attr('class', 'entity-link')
            .attr('ui-sref', 'apps.detail.overview.programs({ appId: "' + result.label + '"})')
            .text(result.label);

            // TODO: Add ellipsis and tooltip

          let elem = $compile(link)(scope);
          elem.css('top', y(result.label) + margin.top - 1 + (y.rangeBand()/2) + 'px');

          sidebarElem.append(elem);
        });
      }

      // TODO: Add type === 'programs'

    }


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
