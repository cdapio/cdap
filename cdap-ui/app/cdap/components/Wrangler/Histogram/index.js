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

import React, {Component, PropTypes} from 'react';
import shortid from 'shortid';

import Chart from 'chart.js';

export default class Histogram extends Component {
  constructor(props) {
    super(props);

    this.id = shortid.generate();
  }

  componentDidMount() {
    const ctx = document.getElementById(this.id);

    new Chart(ctx, {
      type: 'bar',
      data: {
        labels: this.props.labels,
        datasets: [{
          backgroundColor: 'rgba(255, 102, 0, 0.4)',
          borderColor: '#ff6600',
          hoverBackgroundColor: 'rgba(255, 102, 0, 0.8)',
          borderWidth: 1,
          data: this.props.data
        }]
      },
      options: {
        legend: { display: false },
        scales: {
          xAxes: [{ display: false }],
          yAxes: [{ display: false }]
        }
      }
    });
  }

  render() {
    return (
      <canvas
        id={this.id}
        width="100%"
        height="25"
      />
    );
  }
}

Histogram.propTypes = {
  data: PropTypes.arrayOf(PropTypes.number),
  labels: PropTypes.arrayOf(PropTypes.string)
};
