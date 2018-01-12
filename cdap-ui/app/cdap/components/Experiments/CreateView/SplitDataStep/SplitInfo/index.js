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

import PropTypes from 'prop-types';
import React, { Component } from 'react';
import {connect} from 'react-redux';
import SplitInfoTable from 'components/Experiments/CreateView/SplitDataStep/SplitInfoTable';
import SplitInfoGraph from 'components/Experiments/CreateView/SplitDataStep/SplitInfoGraph';

require('./SplitInfo.scss');

class SplitInfo extends Component {
  static propTypes = {
    splitInfo: PropTypes.object,
    outcome: PropTypes.string,
    activeColumn: PropTypes.string
  };

  state = {
    activeColumn: this.props.outcome,
    splitInfo: this.props.splitInfo
  };

  onActiveColumnChange = (activeColumn) => {
    this.setState({activeColumn});
  };

  componentWillReceiveProps({splitInfo, outcome, schema}) {
    this.setState({
      splitInfo,
      activeColumn: outcome,
      schema
    });
  }

  render() {
    return (
      <div className="split-info">
        <h5> Verify Sample by Feature or Outcome </h5>
        <div className="active-column-container">
          Displaying column: <strong>{this.state.activeColumn}</strong>
        </div>
        <div className="split-info-graph-wrapper">
          <SplitInfoGraph
            splitInfo={this.state.splitInfo}
            activeColumn={this.state.activeColumn}
          />
        </div>
        <SplitInfoTable
          splitInfo={this.state.splitInfo}
          onActiveColumnChange={this.onActiveColumnChange}
        />
      </div>
    );
  }
}

const mapStateToSplitInfoProps = (state) => {
  return {
    splitInfo: state.model_create.splitInfo,
    outcome: state.experiments_create.outcome,
    schema: state.model_create.schema
  };
};

const ConnectedSplitInfo = connect(mapStateToSplitInfoProps)(SplitInfo);

export default ConnectedSplitInfo;
