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
import { objectQuery } from 'services/helpers';
import { IThroughputLatencyData } from 'components/Replicator/Detail/ThroughputLatencyGraphs/parser';
import { timeFormatMonthDate, timeFormatHourMinute } from 'components/ChartContainer';

export const COLOR_MAP = {
  inserts: '#185ABC',
  updates: '#669DF6',
  deletes: '#AECBFA',
  horizontalLine: '#DDDDDD',
  verticalLine: '#979797',
  legend: '#999999',
  tick: '#333333',
};

export function renderThroughputGraph(
  id: string,
  data: IThroughputLatencyData[],
  containerWidth: number
) {
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
  const groupId = 'replication-throughput-graph-container';
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
  const yMax = Math.max(MIN_Y_AXIS, d3.max(data.map((d) => d.inserts + d.updates + d.deletes)));
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
    .tickFormat((d) => {
      // removing decimal ticks
      if (parseInt(d, 10) !== d) {
        return;
      }
      return numeral(d).format('0a');
    });
  const yAxisGroup = chart.append('g').attr('class', 'axis axis-y');
  yAxisGroup.call(yAxis);
  yAxisGroup.select('.domain').remove();
  yAxisGroup.selectAll('line').attr('stroke', COLOR_MAP.horizontalLine);
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
      return i % 6 !== 0;
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

  // GRAPHS
  const barWidth = x.bandwidth();
  const barGroupElem = chart.append('g').attr('class', 'bar-container');

  const bar = barGroupElem
    .selectAll('rect')
    .data(data)
    .enter();

  const xLocation = (d) => x(d.time);
  const insertHeight = (d) => height - y(d.inserts);
  const updatesHeight = (d) => height - y(d.updates);
  const deletesHeight = (d) => height - y(d.deletes);

  const { show, hide } = createTooltip(id);
  svg.on('click', hide);

  // INSERTS
  bar
    .append('rect')
    .attr('class', 'inserts')
    .attr('fill', COLOR_MAP.inserts)
    .attr('width', barWidth)
    .attr('height', insertHeight)
    .attr('x', xLocation)
    .attr('y', (d) => y(d.inserts))
    .on('click', show);

  // UPDATES
  bar
    .append('rect')
    .attr('class', 'updates')
    .attr('fill', COLOR_MAP.updates)
    .attr('width', barWidth)
    .attr('height', updatesHeight)
    .attr('x', xLocation)
    .attr('y', (d) => y(d.updates) - insertHeight(d))
    .on('click', show);

  // DELETES
  bar
    .append('rect')
    .attr('class', 'deletes')
    .attr('fill', COLOR_MAP.deletes)
    .attr('width', barWidth)
    .attr('height', deletesHeight)
    .attr('x', xLocation)
    .attr('y', (d) => y(d.deletes) - insertHeight(d) - updatesHeight(d))
    .on('click', show);

  // LEGEND
  chart
    .append('g')
    .attr('class', 'legend axis-y-left-legend')
    .append('text')
    .attr('transform', `translate(-${margin.left / 2}, ${height / 2}) rotate(-90)`)
    .text('events')
    .style('font-size', '12px')
    .style('fill', COLOR_MAP.legend);
}

function createTooltip(id) {
  const tooltip = d3
    .select(`#${id}`)
    .append('div')
    .style('opacity', 0)
    .attr('class', 'graph-tooltip')
    .style('background-color', 'white')
    .style('border', 'solid')
    .style('border-width', '2px')
    .style('border-radius', '4px')
    .style('border-color', COLOR_MAP.legend)
    .style('padding', '5px')
    .style('position', 'absolute');

  function show(d) {
    const htmlElem = `
      <div>
        <strong>Time: ${timeFormatMonthDate(d.time)}</strong>
        <div>
          <div>Inserts: ${d.inserts}</div>
          <div>Updates: ${d.updates}</div>
          <div>Deletes: ${d.deletes}</div>
          <div>Errors: ${d.errors}</div>
        </div>
      </div>
    `;
    tooltip
      .html(htmlElem)
      .style('left', d3.mouse(this)[0] + 'px')
      .style('top', d3.mouse(this)[1] + 'px')
      .style('opacity', 1);
  }

  function hide() {
    const target = objectQuery(d3.event, 'target', 'classList', 'value');
    if (['inserts', 'updates', 'deletes'].indexOf(target) !== -1) {
      return;
    }
    tooltip.style('opacity', 0);
  }

  return {
    show,
    hide,
  };
}
