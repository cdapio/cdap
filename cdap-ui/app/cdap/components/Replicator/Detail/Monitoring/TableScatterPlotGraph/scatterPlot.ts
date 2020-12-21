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
import { ITableMetricsData } from 'components/Replicator/Detail/Monitoring/TableScatterPlotGraph/parser';
import numeral from 'numeral';
import { tooltipWidth } from 'components/Replicator/Detail/Monitoring/TableScatterPlotGraph/ScatterPlotTooltip';
import { ITableInfo } from 'components/Replicator/types';
import { getFullyQualifiedTableName } from 'components/Replicator/utilities';

export const COLOR_MAP = {
  active: '#0076DC',
  activeOutline: '#9FC0FB',
  inactive: '#999999',
  inactiveOutline: '#686868',
  error: '#D40001',
  errorOutline: '#A40403',
  axisColor: '#DDDDDD',
  legend: '#999999',
  tick: '#333333',
};

export function renderScatterPlot(
  activeTableInfo: ITableInfo,
  id: string,
  data: ITableMetricsData[],
  containerWidth: number,
  unusedHeight?: number,
  onHover?: (top, left, isOpen, data) => void,
  onClick?: (top, left, isOpen, data) => void
) {
  const containerHeight = 350;

  const margin = {
    top: 10,
    bottom: 50,
    left: 60,
    right: 35,
  };

  const width = containerWidth - margin.left - margin.right;
  if (width < 0) {
    return;
  }

  const height = containerHeight - margin.top - margin.bottom;

  const activeTable = activeTableInfo ? getFullyQualifiedTableName(activeTableInfo) : null;
  let isClicked = !!activeTable;

  const svg = d3
    .select(`#${id} > svg`)
    .attr('height', containerHeight)
    .attr('width', containerWidth);

  // Clear out existing graph
  const groupId = 'replication-latency-graph-con13tainer';
  const groupElem = d3.select(`#${id} > svg > #${groupId}`);
  groupElem.remove();

  // Start graph render
  const chart = svg
    .append('g')
    .attr('id', groupId)
    .attr('transform', `translate(${margin.left}, ${margin.top})`);

  const GRAPH_BUFFER = 1.25;
  const MIN_VALUE = 1;

  const xMax = Math.max(
    MIN_VALUE,
    d3.max(data, (d) => d.eventsPerMin)
  );
  const x = d3
    .scaleLinear()
    .domain([0, xMax * GRAPH_BUFFER])
    .rangeRound([0, width])
    .nice();

  const yMax = Math.max(
    MIN_VALUE,
    d3.max(data, (d) => d.latency)
  );
  const y = d3
    .scaleLinear()
    .domain([0, yMax * GRAPH_BUFFER])
    .rangeRound([height, 0])
    .nice();

  // AXIS
  const yAxis = d3
    .axisLeft(y)
    .ticks(null, 's')
    .tickFormat(tickFormat);
  const yAxisGroup = chart.append('g').attr('class', 'axis axis-y');
  yAxisGroup.call(yAxis);
  // yAxisGroup.select('.domain').remove();
  yAxisGroup.selectAll('line').attr('stroke', COLOR_MAP.axisColor);
  const yTicks = yAxisGroup.selectAll('.tick');
  const ticksSet = new Set();
  yTicks
    .filter((d) => {
      const currentTick = tickFormat(d);
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
    .style('fill', COLOR_MAP.tick)
    .attr('transform', 'translate(-5, 0)');

  const xAxis = d3
    .axisBottom(x)
    .ticks(null, 's')
    .tickSizeInner(-height)
    .tickSizeOuter(3)
    .tickFormat(tickFormat);
  const xAxisGroup = chart
    .append('g')
    .attr('class', 'axis axis-x')
    .attr('transform', `translate(0, ${height})`);
  xAxisGroup.call(xAxis);
  xAxisGroup.select('.domain').attr('stroke', COLOR_MAP.axisColor);
  xAxisGroup.selectAll('line').attr('stroke', COLOR_MAP.axisColor);
  xAxisGroup
    .selectAll('.tick text')
    .style('font-size', '12px')
    .style('fill', COLOR_MAP.tick)
    .attr('transform', 'translate(0, 10)');

  // GRAPHS
  const scatterGroupElem = chart.append('g').attr('class', 'scatter-plot-container');
  const scatter = scatterGroupElem
    .selectAll('circle')
    .data(data)
    .enter();

  scatter
    .append('circle')
    .style('cursor', 'pointer')
    .attr('cx', (d) => x(d.eventsPerMin))
    .attr('cy', (d) => y(d.latency))
    .attr('r', 10)
    .attr('fill', getFillColor)
    .attr('stroke-width', 1)
    .attr('stroke', getStrokeColor)
    .on('mouseover', (d) => {
      const { top, left } = getTooltipPosition(d);
      onHover(top, left, true, d);
    })
    .on('mouseout', (d) => {
      if (isClicked) {
        return;
      }
      onHover(0, 0, false, d);
    })
    .on('click', (d) => {
      addOverlay(d);
    });

  function getTooltipPosition(d) {
    const topOffset = 10;
    const top = y(d.latency) - (margin.top + margin.bottom) - topOffset + 'px';
    let left = x(d.eventsPerMin) + margin.left;
    if (left + tooltipWidth >= width) {
      left = left - (left + tooltipWidth - width);
    }
    left = left + 'px';

    return {
      top,
      left,
    };
  }

  // LEGEND
  chart
    .append('g')
    .attr('class', 'legend axis-y-left-legend')
    .append('text')
    .attr('transform', `translate(-${margin.left / 2 + 10}, ${height / 2}) rotate(-90)`)
    .attr('text-anchor', 'middle')
    .text('Latency (min)')
    .style('font-size', '12px')
    .style('fill', COLOR_MAP.legend);

  chart
    .append('g')
    .attr('class', 'legend axis-x-legend')
    .append('text')
    .attr('transform', `translate(${width / 2}, ${height + 40})`)
    .attr('text-anchor', 'middle')
    .text('Throughput (events/min)')
    .style('font-size', '12px')
    .style('fill', COLOR_MAP.legend);

  const overlay = chart.append('g').attr('class', 'overlay');
  const overlayContentSelector = 'overlay-content';

  if (activeTable) {
    const activeData = data.find((d) => d.tableName === activeTable);

    if (activeData) {
      addOverlay(activeData);
    }
  } else {
    removeOverlay();
  }

  function addOverlay(d) {
    isClicked = true;
    const { top, left } = getTooltipPosition(d);
    onClick(top, left, true, d);

    const overlayContent = overlay.append('g').attr('class', overlayContentSelector);
    overlayContent
      .append('rect')
      .attr('fill', 'white')
      .attr('opacity', 0.8)
      .attr('width', width)
      .attr('height', height)
      .on('click', removeOverlay);

    // crosshair
    overlayContent
      .append('line')
      .attr('stroke', COLOR_MAP.inactiveOutline)
      .attr('stroke-width', 1)
      .attr('x1', 0)
      .attr('x2', width)
      .attr('y1', y(d.latency))
      .attr('y2', y(d.latency));
    overlayContent
      .append('line')
      .attr('stroke', COLOR_MAP.inactiveOutline)
      .attr('stroke-width', 1)
      .attr('x1', x(d.eventsPerMin))
      .attr('x2', x(d.eventsPerMin))
      .attr('y1', 0)
      .attr('y2', height);

    overlayContent
      .append('circle')
      .attr('cx', x(d.eventsPerMin))
      .attr('cy', y(d.latency))
      .attr('r', 10)
      .attr('fill', getFillColor(d))
      .attr('stroke-width', 1)
      .attr('stroke', getStrokeColor(d));
  }

  function removeOverlay() {
    isClicked = false;
    overlay.select(`.${overlayContentSelector}`).remove();
    onClick(0, 0, false, null);
  }
}

function isInactive(d) {
  return d.latency === 0 && d.eventsPerMin === 0;
}

function getFillColor(d) {
  if (d.errors > 0) {
    return COLOR_MAP.error;
  } else if (isInactive(d)) {
    return COLOR_MAP.inactive;
  }
  return COLOR_MAP.active;
}

function getStrokeColor(d) {
  if (d.errors > 0) {
    return COLOR_MAP.errorOutline;
  } else if (isInactive(d)) {
    return COLOR_MAP.inactiveOutline;
  }
  return COLOR_MAP.activeOutline;
}

function tickFormat(d) {
  // removing decimal ticks
  if (parseInt(d, 10) !== d) {
    return;
  }
  return numeral(d).format('0a');
}
