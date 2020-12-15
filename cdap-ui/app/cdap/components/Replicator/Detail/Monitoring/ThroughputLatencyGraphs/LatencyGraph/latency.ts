/*
 * Copyright © 2020 Cask Data, Inc.
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
import numeral from 'numeral';
import { IThroughputLatencyData } from 'components/Replicator/Detail/Monitoring/ThroughputLatencyGraphs/parser';
import { timeFormatMonthDate, timeFormatHourMinute } from 'components/ChartContainer';
import { tooltipWidth } from 'components/Replicator/Detail/Monitoring/ThroughputLatencyGraphs/LatencyGraph/LatencyTooltip';

export interface ILatencyData {
  time: number;
  latency: number;
}

export const COLOR_MAP = {
  line: '#13B5CC',
  area: 'rgba(161, 228, 242, 0.6)',
  horizontalLine: '#DDDDDD',
  verticalLine: '#979797',
  legend: '#999999',
  tick: '#333333',
};

export function renderLatencyGraph(
  id: string,
  data: IThroughputLatencyData[],
  containerWidth: number,
  unusedHeight?: number,
  onHover?: (top, left, isOpen, activeData) => void
) {
  if (containerWidth < 0) {
    return;
  }
  const containerHeight = 300;

  const margin = {
    top: 10,
    bottom: 40,
    left: 60,
    right: 35,
  };

  const width = containerWidth - margin.left - margin.right;
  const height = containerHeight - margin.top - margin.bottom;

  const svg = d3
    .select(`#${id} > svg`)
    .attr('height', containerHeight)
    .attr('width', containerWidth);

  // Clear out existing graph
  const groupId = 'replication-latency-graph-con13tainer';
  const groupElem = d3.select(`#${id} > svg > #${groupId}`);
  groupElem.remove();

  // remove existing tooltip
  d3.select(`#${id} > .graph-tooltip`).remove();

  // Start graph render
  const chart = svg
    .append('g')
    .attr('id', groupId)
    .attr('transform', `translate(${margin.left}, ${margin.top})`);

  const x = d3
    .scaleBand()
    .domain(data.map((d) => d.time))
    .range([0, width])
    .padding(0.1);

  const MIN_Y_AXIS = 1;
  const yMax = Math.max(MIN_Y_AXIS, d3.max(data.map((d) => d.latency)));
  const Y_BUFFER = 1.25;
  const y = d3
    .scaleLinear()
    .domain([0, yMax * Y_BUFFER])
    .rangeRound([height, 0])
    .nice();

  // AXIS
  const yAxis = d3
    .axisLeft(y)
    .ticks(null, 's')
    .tickSizeInner(-width)
    .tickFormat(yTickFormat);
  const yAxisGroup = chart.append('g').attr('class', 'axis axis-y');
  yAxisGroup.call(yAxis);
  yAxisGroup.select('.domain').remove();
  yAxisGroup.selectAll('line').attr('stroke', COLOR_MAP.horizontalLine);
  const yTicks = yAxisGroup.selectAll('.tick');
  const ticksSet = new Set();
  yTicks
    .filter((d) => {
      const currentTick = yTickFormat(d);
      if (ticksSet.has(currentTick)) {
        return true;
      }

      ticksSet.add(currentTick);
      return false;
    })
    .remove();
  yAxisGroup
    .selectAll('.tick text')
    .style('font-size', '12px')
    .style('fill', COLOR_MAP.tick);

  const xAxis = d3
    .axisBottom(x)
    .tickSizeOuter(0)
    .tickFormat(timeFormatMonthDate);
  const xAxisGroup = chart
    .append('g')
    .attr('class', 'axis axis-x')
    .attr('transform', `translate(0, ${height})`);
  xAxisGroup.call(xAxis);
  xAxisGroup.select('.domain').attr('stroke', COLOR_MAP.horizontalLine);
  xAxisGroup.selectAll('line').attr('stroke', COLOR_MAP.verticalLine);
  xAxisGroup
    .selectAll('.tick')
    .filter((d, i) => {
      return i % Math.floor(data.length / 4) !== 0;
    })
    .remove();
  xAxisGroup
    .selectAll('.tick')
    .append('text')
    .text(timeFormatHourMinute)
    .attr('dy', 30);
  xAxisGroup
    .selectAll('.tick text')
    .style('font-size', '12px')
    .style('fill', COLOR_MAP.tick);

  // GRAPH
  const area = d3
    .area()
    .x((d) => x(d.time))
    .y1((d) => y(d.latency))
    .y0(y(0));

  const line = d3
    .line()
    .x((d) => x(d.time))
    .y((d) => y(d.latency));

  const areaGroup = chart
    .append('g')
    .attr('class', 'area-graph')
    .attr('transform', `translate(${x.bandwidth() / 2}, 0)`);

  areaGroup
    .append('path')
    .datum(data)
    .attr('class', 'latency-path')
    .attr('d', area)
    .style('fill', COLOR_MAP.area);

  const pathGroup = chart
    .append('g')
    .attr('class', 'line-graph')
    .attr('transform', `translate(${x.bandwidth() / 2}, 0)`);

  pathGroup
    .append('path')
    .datum(data)
    .attr('class', 'latency-path')
    .attr('d', line)
    .style('fill', 'none')
    .style('stroke', COLOR_MAP.line)
    .style('stroke-width', '4px');

  // LEGEND
  chart
    .append('g')
    .attr('class', 'legend axis-y-left-legend')
    .append('text')
    .attr('transform', `translate(-${margin.left / 2}, ${height / 2}) rotate(-90)`)
    .text('Minutes')
    .style('font-size', '12px')
    .style('fill', COLOR_MAP.legend);

  // Hover
  const hoverContainer = chart
    .append('g')
    .attr('class', 'hover-container')
    .selectAll('rect')
    .data(data)
    .enter();

  const hoverIndicatorContainer = chart.append('g').attr('class', 'hover-indicator-container');
  const barWidth = x.bandwidth();

  hoverContainer
    .append('rect')
    .attr('height', height)
    .attr('width', barWidth)
    .attr('opacity', 0)
    .attr('x', (d) => x(d.time))
    .on('mouseover', (d) => {
      const tooltipTopOffset = 75;
      const tooltipLeftOffset = 100;
      const top = y(d.latency) - tooltipTopOffset + 'px';
      let left = x(d.time) + tooltipLeftOffset;
      if (left + tooltipWidth >= width) {
        left = left - (left + tooltipWidth - width);
      }
      left = left + 'px';
      onHover(top, left, true, d);

      const xPosition = x(d.time) + barWidth / 2;

      hoverIndicatorContainer
        .append('line')
        .attr('stroke', COLOR_MAP.tick)
        .attr('stroke-width', 1)
        .attr('x1', xPosition)
        .attr('x2', xPosition)
        .attr('y1', 0)
        .attr('y2', height);
    })
    .on('mouseout', (d) => {
      onHover(0, 0, false, d);
      hoverIndicatorContainer.selectAll('line').remove();
    });
}

function yTickFormat(d) {
  // removing decimal ticks
  if (parseInt(d, 10) !== d) {
    return;
  }
  return numeral(Math.floor(d / 60)).format('0a');
}
