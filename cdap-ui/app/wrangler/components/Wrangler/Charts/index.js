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

import React, { Component, PropTypes } from 'react';
import WranglerStore from 'wrangler/components/Wrangler/Store/WranglerStore';

require('./c3.scss');

export default class Charts extends Component {
  constructor(props) {
    super(props);

    this.chart = null;
  }

  componentDidMount() {
    this.createGraph();
  }

  componentDidUpdate() {
    this.createGraph();
  }

  createGraph() {
    if (this.chart) {
      this.chart = this.chart.destroy();
    }

    const data = WranglerStore.getState().wrangler.data;

    let chartSpec = {
      bindto: `#${this.props.spec.id}`,
      data: {
        json: data,
        keys: {
          value: this.props.spec.y
        },
        type: this.props.spec.type
      },
      axis: {
        y: {
          text: this.props.spec.y,
          position: 'outer-middle'
        },
        x: {
          type: 'category',
          tick: {
            fit: true,
            multiline: false,
            count: 5,
            culling: {
              max: 5
            }
          }
        }
      }
    };

    if (this.props.spec.x !== '##') {
      chartSpec.data.keys.x = this.props.spec.x;
    }

    this.chart = c3.generate(chartSpec);
  }

  render() {
    return (
      <div id={this.props.spec.id}></div>
    );
  }
}


// TODO: be more specific
Charts.propTypes = {
  spec: PropTypes.object
};
