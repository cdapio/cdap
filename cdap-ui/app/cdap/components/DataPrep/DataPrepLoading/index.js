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

import React, { Component } from 'react';
import DataPrepStore from 'components/DataPrep/store';

require('./DataPrepLoading.scss');

export default class DataPrepLoading extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: DataPrepStore.getState().dataprep.loading
    };
  }

  componentWillMount() {
    this.sub = DataPrepStore.subscribe(() => {
      this.setState({loading: DataPrepStore.getState().dataprep.loading});
    });
  }

  componentWillUnmount() {
    this.sub();
  }

  render() {
    if (!this.state.loading) { return null; }

    // SVG loading will need to be extracted out

    return (
      <div className="dataprep-loading text-xs-center">
        <svg
          className="loading-bar"
          version="1.1"
          x="0px"
          y="0px"
          width="24px"
          height="30px"
          viewBox="0 0 24 30"
          style={{'enable-background': 'new 0 0 50 50'}}
        >
          <rect
            x="0"
            y="10"
            width="4"
            height="10"
            fill="#333"
            opacity="0.2"
          >
            <animate
              attributeName="opacity"
              attributeType="XML"
              values="0.2; 1; .2"
              begin="0s"
              dur="0.6s"
              repeatCount="indefinite"
            />
            <animate
              attributeName="height"
              attributeType="XML"
              values="10; 20; 10"
              begin="0s"
              dur="0.6s"
              repeatCount="indefinite"
            />
            <animate
              attributeName="y"
              attributeType="XML"
              values="10; 5; 10"
              begin="0s"
              dur="0.6s"
              repeatCount="indefinite"
            />
          </rect>
          <rect
            x="8"
            y="10"
            width="4"
            height="10"
            fill="#333"
            opacity="0.2"
          >
            <animate
              attributeName="opacity"
              attributeType="XML"
              values="0.2; 1; .2"
              begin="0.15s"
              dur="0.6s"
              repeatCount="indefinite"
            />
            <animate
              attributeName="height"
              attributeType="XML"
              values="10; 20; 10"
              begin="0.15s"
              dur="0.6s"
              repeatCount="indefinite"
            />
            <animate
              attributeName="y"
              attributeType="XML"
              values="10; 5; 10"
              begin="0.15s"
              dur="0.6s"
              repeatCount="indefinite"
            />
          </rect>
          <rect
            x="16"
            y="10"
            width="4"
            height="10"
            fill="#333"
            opacity="0.2"
          >
            <animate
              attributeName="opacity"
              attributeType="XML"
              values="0.2; 1; .2"
              begin="0.3s"
              dur="0.6s"
              repeatCount="indefinite"
            />
            <animate
              attributeName="height"
              attributeType="XML"
              values="10; 20; 10"
              begin="0.3s"
              dur="0.6s"
              repeatCount="indefinite"
            />
            <animate
              attributeName="y"
              attributeType="XML"
              values="10; 5; 10"
              begin="0.3s"
              dur="0.6s"
              repeatCount="indefinite"
            />
          </rect>
        </svg>
      </div>
    );
  }
}
