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
  .directive('myHistogram', function (d3, moment) {

    function getXaxisDomain(start, end) {
      var pattern = /\d+/g,
          startTime,
          endTime;
      // _time is days in string extracted from 'now-5d'
      var getTimeInEpoch = function(_time) {
        let _t = parseInt(_time, 10);
        if (Number.isNaN(_t)) {
          return Date.now();
        }

        let tempDate = new Date();
        tempDate = tempDate.setDate( tempDate.getDate() - _t );  // 20 days from now
        return tempDate;
      };
      if (typeof start === 'string' && start.indexOf('now') !== -1) {
        startTime = start.match(pattern);
        if (Array.isArray(startTime)) {
          startTime = startTime[0];
        }
        startTime = getTimeInEpoch(startTime);
      } else {
        startTime = parseInt(start, 10) * 1000;
      }
      if (typeof end === 'string' && start.indexOf('now') !== -1) {
        endTime = end.match(pattern);
        if (Array.isArray(endTime)) {
          endTime = endTime[0];
        }
        endTime = getTimeInEpoch(endTime);
      } else {
        endTime = parseInt(end, 10) * 1000;
      }
      return { 'startTime': startTime, 'endTime': endTime };
    }
    function HistogramLink (scope, element) {
      scope.render = function() {
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
            count: d.value
          };
        });

        let x = d3.time.scale()
          .range([0, width]);

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
        let xDomain = getXaxisDomain(scope.startTime, scope.endTime);
        x.domain([ xDomain.startTime,xDomain.endTime ]);
        y.domain([0, d3.max(data, (d) => { return d.count; })]).nice();

        /* X AXIS */
        let timeFormat = d3.time.format('%b %d, %Y');
        let xAxis = d3.svg.axis()
          .scale(x)
          .orient('bottom')
          .tickFormat(timeFormat)
          .outerTickSize(0);

        let durationAsDays = moment.duration(xDomain.endTime - xDomain.startTime).asDays();
        if (durationAsDays >= 179) { // 6 months
          xAxis.ticks(d3.time.months, 1).tickFormat(d3.time.format('%b %Y'));
        } else if ( durationAsDays >= 30 && durationAsDays < 179) {
          xAxis.ticks(d3.time.weeks, 1).tickFormat(d3.time.format('%b-%e'));
        } else {
          xAxis.ticks(d3.time.days, 1).tickFormat(d3.time.format('%b-%e'));
        }
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
          .ticks(10);

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
            .attr('x', d => {
              return x(d.time) - (((width + 130) / durationAsDays));
            })
            .attr('y', (d) => { return y(d.count); })
            .attr('width', () => {
              return (width - 180) / durationAsDays;
            })
            .attr('height', (d) => { return height - y(d.count); })
          .on('mouseover', tip.show)
          .on('mouseout', tip.hide);
      };
    }

    return {
      restrict: 'E',
      scope: {
        model: '=',
        startTime: '@',
        endTime: '@'
      },
      templateUrl: '/assets/features/tracker/directives/histogram/histogram.html',
      link: HistogramLink,
      controller: function($scope) {
        $scope.$watch('model', function() {
          if (typeof $scope.model === 'object') {
            $scope.render();
          }
        });
      }
    };
  });
