/*
 * Copyright © 2018 Cask Data, Inc.
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

import * as d3 from 'd3';
import DashboardStore, {DashboardActions} from 'components/OpsDashboard/store/DashboardStore';
import moment from 'moment';

const AXIS_BUFFER = 1.1;

export function renderGraph(selector, containerWidth, containerHeight, data, legends) {
  let {
    manual,
    schedule,
    running,
    success,
    failed,
    delay
  } = legends;

  let margin = {
    top: 20,
    right: 50,
    bottom: 30,
    left: 50
  };
  let width = containerWidth - margin.left - margin.right,
      height = containerHeight - margin.top - margin.bottom;

  let svg = d3.select(selector)
    .attr('height', containerHeight)
    .attr('width', containerWidth);

  // Clear out existing graph
  let groupElem = d3.select(`${selector} > .graph-group-container`);
  groupElem.remove();

  // Start graph render
  let chart = svg.append('g')
      .attr('class', 'graph-group-container')
      .attr('transform', `translate(${margin.left}, ${margin.top})`);

  let x = d3.scaleBand()
    .rangeRound([0, width])
    .paddingInner(0)
    .paddingOuter(0);

  let dateX = d3.scaleBand()
    .rangeRound([0, width])
    .paddingInner(0)
    .paddingOuter(0);

  let yLeft = d3.scaleLinear()
    .rangeRound([height, 0]);

  let yRight = d3.scaleLinear()
    .rangeRound([height, 0]);

  // SETTING DOMAINS
  x.domain(data.map((d) => d.time));

  dateX.domain(data.map((d) => d.time));

  let yLeftMax = d3.max(data, (d) => Math.max(d.manual + d.schedule, d.running + d.successful + d.failed));
  let yRightMax = d3.max(data, (d) => d.delay);
  yLeft.domain([0, yLeftMax * AXIS_BUFFER]);
  yRight.domain([0, yRightMax * AXIS_BUFFER]);


  // RENDER AXIS
  let barPadding = 2;
  let barWidth = (x.bandwidth() - barPadding * 6) / 2;

  // X Axis
  let xAxis = d3.axisBottom(x)
    .tickSizeInner(-height)
    .tickSizeOuter(0)
    .tickFormat(d3.timeFormat('%-I %p'));

  let axisOffset = barWidth + barPadding * 2;
  let xAxisGroup = chart.append('g')
    .attr('class', 'axis axis-x')
    .attr('transform', `translate(${axisOffset}, ${height})`)
    .call(xAxis);

  xAxisGroup.select('.domain')
    .remove();

  xAxisGroup.selectAll('text')
    .attr('x', -axisOffset)
    .attr('y', 10);

  xAxisGroup.select('.tick:last-child')
    .select('line')
    .remove();

  let dateMap = {};
  data.forEach((d) => {
    let time = parseInt(d.time, 10);
    let key = moment(time).format('ddd. MMM D, YYYY');
    if (!dateMap[key]) {
      dateMap[key] = 0;
    }
    dateMap[key]++;
  });

  let dates = Object.keys(dateMap);
  let firstDateIndex = Math.floor(dateMap[dates[0]] / 2) || 1;
  let secondDateIndex = Math.floor(dateMap[dates[0]] + (dateMap[dates[1]] / 2));

  let dateAxis = d3.axisTop(dateX)
    .tickFormat(d3.timeFormat('%a. %b %e, %Y'))
    .tickSizeInner(0)
    .tickSizeOuter(0);

  let dateAxisGroup = chart.append('g')
    .attr('class', 'axis axis-date')
    .call(dateAxis)
      .selectAll('.tick text')
      .attr('class', 'date-axis-tick')
      .filter((d, i) => {
        return i !== firstDateIndex && i !== secondDateIndex;
      })
      .text(null);

  dateAxisGroup.select('.domain')
    .remove();

  // Y Axis Left
  chart.append('g')
    .attr('class', 'axis axis-y-left')
    .call(d3.axisLeft(yLeft).tickFormat((e) => {
      // showing only integers
      if (Math.floor(e) !== e) { return; }
      return e;
    }));

  // Y Axis Right
  chart.append('g')
    .attr('class', 'axis axis-y-right')
    .attr('transform', `translate(${width}, 0)`)
    .call(d3.axisRight(yRight).tickSizeOuter(-width));


  // RENDER BAR GRAPH

  // Start method
  let barStartMethod = chart.append('g')
    .attr('class', 'bar-start-method');

  let startMethod = barStartMethod.selectAll('rect')
    .data(data)
    .enter();

  // Schedule
  if (schedule) {
    startMethod.append('rect')
      .attr('class', 'bar schedule')
      .attr('width', barWidth)
      .attr('x', getXLocation)
      .attr('y', (d) => yLeft(d.schedule))
      .attr('height', (d) => height - yLeft(d.schedule));
  }

  // Manual
  if (manual) {
    startMethod.append('rect')
      .attr('class', 'bar manual')
      .attr('width', barWidth)
      .attr('x', getXLocation)
      .attr('y', getManualY)
      .attr('height', (d) => height - yLeft(d.manual));
  }


  // Statistics
  let statisticsOffsetX = barWidth + barPadding;
  let barStatistics = chart.append('g')
    .attr('class', 'bar-statistics')
    .attr('transform', `translate(${statisticsOffsetX}, 0)`);


  let statistics = barStatistics.selectAll('rect')
    .data(data)
    .enter();

  // Running
  if (running) {
    statistics.append('rect')
      .attr('class', 'bar running')
      .attr('width', barWidth)
      .attr('x', getXLocation)
      .attr('y', (d) => yLeft(d.running))
      .attr('height', (d) => height - yLeft(d.running));
  }

  // Successful
  if (success) {
    statistics.append('rect')
      .attr('class', 'bar successful')
      .attr('width', barWidth)
      .attr('x', getXLocation)
      .attr('y', getSuccessY)
      .attr('height', (d) => height - yLeft(d.successful));
  }

  // Failed
  if (failed) {
    statistics.append('rect')
      .attr('class', 'bar failed')
      .attr('width', barWidth)
      .attr('x', getXLocation)
      .attr('y', getFailedY)
      .attr('height', (d) => height - yLeft(d.failed));
  }


  // RENDER LINE GRAPH
  if (delay) {
    let line = d3.line()
      .x((d) => x(d.time))
      .y((d) => yRight(d.delay));

    let offsetX = barWidth + barPadding * 2;

    let pathGroup = chart.append('g')
      .attr('class', 'line-graph')
      .attr('transform', `translate(${offsetX}, 0)`);

    pathGroup.append('path')
      .datum(data)
      .attr('class', 'delay-path')
      .attr('d', line);

    pathGroup.append('g')
        .attr('class', 'dots')
      .selectAll('circle')
        .data(data)
      .enter().append('circle')
        .attr('class', 'dot delay-dot')
        .attr('r', 5)
        .attr('cx', (d) => x(d.time))
        .attr('cy', (d) => yRight(d.delay));
  }


  // HOVER AND CLICK HANDLER
  let stepWidth = x.bandwidth();
  let handlerGroup = chart.append('g')
    .attr('class', 'handler');

  let handler = handlerGroup.selectAll('rect')
    .data(data)
    .enter();

  handler.append('rect')
    .attr('class', 'handler-selector pointer')
    .attr('opacity', 0)
    .attr('data', (d) => d.time)
    .attr('width', stepWidth)
    .attr('x', getXLocation)
    .attr('height', height);

  d3.selectAll('.handler-selector')
    .on('click', (data) => {
      DashboardStore.dispatch({
        type: DashboardActions.setDisplayBucket,
        payload: {
          displayBucketInfo: data
        }
      });
    });


  // Helper functions
  function getXLocation(d) {
    return x(d.time) + barPadding * 2;
  }

  function getManualY(d) {
    let y = yLeft(d.manual);
    if (schedule) {
      y = y - (height - yLeft(d.schedule));
    }
    return y;
  }

  function getSuccessY(d) {
    let y = yLeft(d.successful);
    if (running) {
      y = y - (height - yLeft(d.running));
    }
    return y;
  }

  function getFailedY(d) {
    let y = yLeft(d.failed);
    if (running) {
      y = y - (height - yLeft(d.running));
    }
    if (success) {
      y = y - (height - yLeft(d.successful));
    }
    return y;
  }
}
