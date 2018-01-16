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

import React, { Component } from 'react';
import {renderGraph} from 'components/OpsDashboard/RunsGraph/graphRenderer';

import DATA from './data';

require('./RunsGraph.scss');

export default class ClassName extends Component {
  state = {
    data: []
  };

  componentWillMount() {
    let data = DATA.split('\n').map((line) => {
      let split = line.split(',');
      let time = split.shift();

      split = split.map(item => parseInt(item, 10));

      return {
        time,
        manual: split[0],
        schedule: split[1],
        running: split[2],
        successful: split[3],
        failed: split[4],
        delay: split[5]
      };
    });

    this.setState({data});
  }

  componentDidMount() {
    let width = 1100,
        height = 400;
    renderGraph('#runs-graph', width, height, this.state.data);
  }

  render() {
    return (
      <div className="runs-graph-container">
        <h1 className="text-xs-center">
          Runs Graph
        </h1>

        <div className="runs-graph">
          <svg id="runs-graph" />
        </div>
      </div>
    );
  }
}
