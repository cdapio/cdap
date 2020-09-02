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
import DashboardStore, {
  DashboardActions,
  ViewByOptions,
} from 'components/OpsDashboard/store/DashboardStore';
import moment from 'moment-timezone';
import { humanReadableDate } from 'services/helpers';
import numeral from 'numeral';

const AXIS_BUFFER = 1.1;
const MIN_Y_AXIS = 1;

export function renderGraph(selector, containerWidth, containerHeight, data, viewByOption) {
  let margin = {
    top: 45,
    right: 60,
    bottom: 40,
    left: 60,
  };
  let width = containerWidth - margin.left - margin.right,
    height = containerHeight - margin.top - margin.bottom;

  let svg = d3
    .select(selector)
    .attr('height', containerHeight)
    .attr('width', containerWidth);

  // Clear out existing graph
  let groupElem = d3.select(`${selector} > .graph-group-container`);
  groupElem.remove();

  // Start graph render
  let chart = svg
    .append('g')
    .attr('class', 'graph-group-container')
    .attr('transform', `translate(${margin.left}, ${margin.top})`);

  let x = d3
    .scaleBand()
    .rangeRound([0, width])
    .paddingInner(0)
    .paddingOuter(0);

  let dateX = d3
    .scaleBand()
    .rangeRound([0, width])
    .paddingInner(0)
    .paddingOuter(0);

  let yLeft = d3.scaleLinear().range([height, 0]);

  let yRight = d3.scaleLinear().range([height, 0]);

  // SETTING DOMAINS
  x.domain(data.map((d) => d.time));

  dateX.domain(data.map((d) => d.time));

  let yLeftMax;

  if (viewByOption === ViewByOptions.startMethod) {
    yLeftMax = d3.max(data, (d) => d.manual + d.schedule);
  } else {
    yLeftMax = d3.max(data, (d) => Math.max(d.running, d.successful + d.failed));
  }

  let yRightMax = d3.max(data, (d) => d.delay);

  yLeftMax = Math.max(yLeftMax, MIN_Y_AXIS);
  yRightMax = Math.max(yRightMax, MIN_Y_AXIS);

  yLeft.domain([0, yLeftMax * AXIS_BUFFER]).nice();
  yRight.domain([0, yRightMax * AXIS_BUFFER]).nice();

  // RENDER AXIS
  let barPadding = 2;
  let stepWidth = x.bandwidth();
  let barWidth = (stepWidth - barPadding * 6) / 2;

  // X Axis
  let xAxis = d3
    .axisBottom(x)
    .tickSizeInner(-height)
    .tickSizeOuter(0)
    .tickFormat(d3.timeFormat('%-I %p'));

  let axisOffset = barWidth + barPadding * 2;
  let xAxisGroup = chart
    .append('g')
    .attr('class', 'axis axis-x')
    .attr('transform', `translate(${axisOffset}, ${height})`)
    .call(xAxis);

  xAxisGroup.select('.domain').remove();

  xAxisGroup
    .selectAll('text')
    .attr('x', -axisOffset)
    .attr('y', 10);

  xAxisGroup
    .select('.tick:last-child')
    .select('line')
    .remove();

  // X Axis Legend
  // need to add some pixels show the legend doesn't appear outside
  // the graph container
  let legendYOffset = margin.top + 2;
  let localTimeZone = moment.tz(moment.tz.guess()).format('z');

  chart
    .append('g')
    .attr('class', 'legend axis-x-legend')
    .append('text')
    .attr('transform', `translate(${width / 2}, ${containerHeight - legendYOffset})`)
    .text(`Time (${localTimeZone})`);

  // Dates axis
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
  let secondDateIndex = Math.floor(dateMap[dates[0]] + dateMap[dates[1]] / 2);

  let dateAxis = d3
    .axisTop(dateX)
    .tickFormat(d3.timeFormat('%a. %b %e, %Y'))
    .tickSizeInner(0)
    .tickSizeOuter(0);

  let dateAxisGroup = chart
    .append('g')
    .attr('class', 'axis axis-date')
    .call(dateAxis);

  dateAxisGroup
    .selectAll('.tick text')
    .attr('class', 'date-axis-tick')
    .filter((d, i) => {
      return i !== firstDateIndex && i !== secondDateIndex;
    })
    .text(null);

  dateAxisGroup.select('.domain').remove();

  // Separator between two dates
  let datesSeparatorIndex = dateMap[dates[0]];

  xAxisGroup
    .select(`.tick:nth-child(${datesSeparatorIndex})`)
    .select('line')
    .attr('stroke-width', '2');

  let tickOffset = stepWidth / 2 - barPadding;

  dateAxisGroup
    .select(`.tick:nth-child(${datesSeparatorIndex})`)
    .select('line')
    .attr('stroke-width', '2')
    .attr('x1', tickOffset)
    .attr('x2', tickOffset)
    .attr('y2', -30);

  // Last updated axis
  let currentTime = humanReadableDate(Date.now(), true);
  let lastUpdatedYOffset = -35;

  chart
    .append('g')
    .attr('class', 'axis-last-updated')
    .append('text')
    .attr('transform', `translate(${width + margin.right}, ${lastUpdatedYOffset})`)
    .text(`Last updated ${currentTime} ${localTimeZone}`);

  // Y Axis Left
  const legendOffset = 30;
  const ticksFontSize = 10;

  chart
    .append('g')
    .attr('class', 'axis axis-y-left')
    .attr('font-size', ticksFontSize)
    .call(d3.axisLeft(yLeft).tickFormat(tickFormat));

  // Y Axis Left Legend
  chart
    .append('g')
    .attr('class', 'legend axis-y-left-legend')
    .append('text')
    .attr('transform', `translate(-${legendOffset + ticksFontSize * 2}, ${height / 2}) rotate(-90)`)
    .text('# of runs');

  // Y Axis Right
  chart
    .append('g')
    .attr('class', 'axis axis-y-right')
    .attr('transform', `translate(${width}, 0)`)
    .attr('font-size', ticksFontSize)
    .call(
      d3
        .axisRight(yRight)
        .tickSizeOuter(-width)
        .tickFormat(tickFormat)
    );

  // Y Axis Right Legend
  chart
    .append('g')
    .attr('class', 'legend axis-y-right-legend')
    .append('text')
    .attr(
      'transform',
      `translate(${width + legendOffset + ticksFontSize * 3}, ${height / 2}) rotate(-90)`
    )
    .text('Delay (seconds)');

  // BUCKETS STYLING LAYER
  let bottomBucketsLayer = chart.append('g').attr('class', 'bottom-buckets-layer');

  let bottomLayer = bottomBucketsLayer
    .selectAll('rect')
    .data(data)
    .enter();

  bottomLayer
    .append('rect')
    .attr('class', 'bucket bottom-layer-bucket')
    .attr('data', (d) => d.time)
    .attr('width', stepWidth)
    .attr('x', (d) => x(d.time))
    .attr('height', height);

  // Since we always select the last bucket by default, highlights it
  d3.select('.bottom-layer-bucket:last-child').classed('selected', true);

  // RENDER BAR GRAPH

  if (viewByOption === ViewByOptions.startMethod) {
    let barStartMethod = chart.append('g').attr('class', 'bar-start-method');

    let startMethod = barStartMethod
      .selectAll('rect')
      .data(data)
      .enter();

    // Schedule
    startMethod
      .append('rect')
      .attr('class', 'bar schedule')
      .attr('width', barWidth * 2)
      .attr('x', getXLocation)
      .attr('y', (d) => yLeft(d.schedule))
      .attr('height', (d) => height - yLeft(d.schedule));

    // Manual
    startMethod
      .append('rect')
      .attr('class', 'bar manual')
      .attr('width', barWidth * 2)
      .attr('x', getXLocation)
      .attr('y', getManualY)
      .attr('height', (d) => height - yLeft(d.manual));
  } else {
    // Running
    let barRunning = chart.append('g').attr('class', 'bar-running');

    let runningStats = barRunning
      .selectAll('rect')
      .data(data)
      .enter();

    runningStats
      .append('rect')
      .attr('class', 'bar running')
      .attr('width', barWidth)
      .attr('x', getXLocation)
      .attr('y', (d) => yLeft(d.running))
      .attr('height', (d) => height - yLeft(d.running));

    // Statistics
    let statisticsOffsetX = barWidth + barPadding;
    let barStatistics = chart
      .append('g')
      .attr('class', 'bar-statistics')
      .attr('transform', `translate(${statisticsOffsetX}, 0)`);

    let statistics = barStatistics
      .selectAll('rect')
      .data(data)
      .enter();

    // Successful
    statistics
      .append('rect')
      .attr('class', 'bar succeeded')
      .attr('width', barWidth)
      .attr('x', getXLocation)
      .attr('y', (d) => yLeft(d.successful))
      .attr('height', (d) => height - yLeft(d.successful));

    // Failed
    statistics
      .append('rect')
      .attr('class', 'bar failed')
      .attr('width', barWidth)
      .attr('x', getXLocation)
      .attr('y', getFailedY)
      .attr('height', (d) => height - yLeft(d.failed));
  }

  // HOVER AND CLICK HANDLER
  let clickHandlersLayer = chart.append('g').attr('class', 'click-handlers-layer');

  let handlerLayer = clickHandlersLayer
    .selectAll('rect')
    .data(data)
    .enter();

  handlerLayer
    .append('rect')
    .attr('class', 'bucket handler-bucket pointer')
    .attr('data', (d) => d.time)
    .attr('opacity', 0)
    .attr('width', stepWidth)
    .attr('x', (d) => x(d.time))
    .attr('height', height);

  let bottomLayerBuckets = d3.selectAll('.bottom-layer-bucket').nodes();

  d3.selectAll('.handler-bucket').on('click', (data, i) => {
    // Need to do this to unselect previously selected bucket
    d3.select('.selected').classed('selected', false);

    d3.select(bottomLayerBuckets[i]).classed('selected', true);

    DashboardStore.dispatch({
      type: DashboardActions.setDisplayBucket,
      payload: {
        displayBucketInfo: data,
      },
    });
  });

  // RENDER LINE GRAPH

  let line = d3
    .line()
    .x((d) => x(d.time))
    .y((d) => yRight(d.delay));

  let offsetX = barWidth + barPadding * 2;

  let pathGroup = chart
    .append('g')
    .attr('class', 'line-graph')
    .attr('transform', `translate(${offsetX}, 0)`);

  pathGroup
    .append('path')
    .datum(data)
    .attr('class', 'delay-path')
    .attr('d', line);

  pathGroup
    .append('g')
    .attr('class', 'dots')
    .selectAll('circle')
    .data(data)
    .enter()
    .append('circle')
    .attr('class', 'dot delay-dot')
    .attr('r', 5)
    .attr('cx', (d) => x(d.time))
    .attr('cy', (d) => yRight(d.delay));

  // Helper functions
  function getXLocation(d) {
    return x(d.time) + barPadding * 2;
  }

  function getManualY(d) {
    let y = yLeft(d.manual);
    y = y - (height - yLeft(d.schedule));
    return y;
  }

  function getFailedY(d) {
    let y = yLeft(d.failed);
    y = y - (height - yLeft(d.successful));
    return y;
  }

  function tickFormat(d) {
    if (d <= 999) {
      // removing decimal ticks
      if (parseInt(d) !== d) {
        return;
      }

      return d;
    }

    return numeral(d).format('0.0a');
  }
}
