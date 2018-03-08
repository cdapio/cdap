/*
 * Copyright © 2017 Cask Data, Inc.
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

import PropTypes from 'prop-types';
import React, {Component} from 'react';
import * as d3Lib from 'd3';
import uuidV4 from 'uuid/v4';
import isNil from 'lodash/isNil';

export default class PieChart extends Component {
  static propTypes = {
    data: PropTypes.arrayOf(PropTypes.shape({
      color: PropTypes.string,
      value: PropTypes.string
    })),
    width: PropTypes.number,
    height: PropTypes.number
  };
  state = {
    data: this.props.data,
    id: `A-${uuidV4()}`
  };
  componentDidMount() {
    this.drawPie();
  }
  componentWillReceiveProps(nextProps) {
    this.setState({data: nextProps.data}, this.drawPie);
  }
  drawPie = () => {
    if (isNil(this.state.data) || (Array.isArray(this.state.data) && !this.state.data.length)) {
      return;
    }
    var svg = d3Lib.select(`#${this.state.id} svg`),
        width = +svg.attr("width"),
        height = +svg.attr("height"),
        radius = Math.min(width, height) / 2,
        g = svg.append("g").attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

    var pie = d3Lib.pie()
        .sort(null)
        .value(function(d) { return d.count; });

    var path = d3Lib.arc()
        .outerRadius(radius - 10)
        .innerRadius(0);

    var arc = g.selectAll(".arc")
        .data(pie(this.state.data))
        .enter().append("g")
          .attr("class", "arc");

    arc.append('title')
      .text((d) => `${d.data.value} (${d.data.count})`);
    arc.append("path")
        .attr("d", path)
        .attr("fill", (d) => d.data.color);
  };
  render() {
    return (
      <div id={`${this.state.id}`}>
        <svg
          width={this.props.width || "50"}
          height={this.props.height || "50"}
        >
        </svg>
      </div>
    );
  }
}
