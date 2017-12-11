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
import React, { Component } from 'react';
import {connect} from 'react-redux';
import {NUMBER_TYPES} from 'services/global-constants';
import StyledSelectTag from 'components/StyledSelectTag';
import AlgorithmDistribution from 'components/Experiments/DetailedView/ExperimentMetricsDropdown/AlgorithmDistribution';
import ModelStatusesDistribution from 'components/Experiments/DetailedView/ExperimentMetricsDropdown/ModelStatusesDistribution';
import MetricChartWithLegend from 'components/Experiments/DetailedView/MetricChartWithLegend';

require('./ExperimentMetricsDropdown.scss');

class ExperimentMetricsDropdown extends Component {
  static propTypes = {
    evaluationMetrics: PropTypes.object,
    algorithms: PropTypes.object,
    statuses: PropTypes.object,
    outcomeType: PropTypes.string
  };
  static commonKeys = [
    {
      id: 'algorithms',
      value: 'Algorithm Types'
    },
    {
      id: 'statuses',
      value: 'Model Status'
    }
  ];
  static regressionKeys = [
    {
      id: 'rmse',
      value: 'RMSE'
    },
    {
      id: 'mae',
      value: 'Mean Avg Error'
    },
    {
      id: 'r2',
      value: 'R2'
    },
    {
      id: 'evariance',
      value: 'Accuracy'
    }
  ];
  static categoricalKeys = [
    {
      id: 'precision',
      value: 'Precision'
    },
    {
      id: 'recall',
      value: 'Recall'
    },
    {
      id: 'f1',
      value: 'F1'
    }
  ];

  state = {
    active: 'algorithms'
  };

  onSelectChange = (e) => {
    this.setState({
      active: e.target.value
    });
  };
  renderMetricBarChart = (metric, label) => {
    if (metric === 'algorithms') {
      return (
        <AlgorithmDistribution algorithms={this.props.algorithms.histo || []}/>
      );
    }
    if (metric === 'statuses') {
      return (
        <ModelStatusesDistribution modelStatuses={this.props.statuses.histo || []} />
      );
    }
    let values = this.props.evaluationMetrics[metric];
    if (!values) {
      return null;
    }
    let {width, height} = this.containerRef.getBoundingClientRect();
    return (
      <MetricChartWithLegend
        xAxisTitle={label}
        values={values.histo}
        width={width}
        height={height}
      />
    );
  };
  render() {
    let keys = [...ExperimentMetricsDropdown.commonKeys];
      keys = NUMBER_TYPES.indexOf(this.props.outcomeType) !== -1 ?
        keys.concat(ExperimentMetricsDropdown.regressionKeys)
      :
        keys.concat(ExperimentMetricsDropdown.categoricalKeys);
    let matchingKey = keys.find(key => key.id === this.state.active);
    return (
      <div className="experiments-metrics-dropdown clearfix" ref={ref => this.containerRef = ref}>
        <StyledSelectTag keys={keys} onChange={this.onSelectChange} />
        {this.renderMetricBarChart(matchingKey.id, matchingKey.value)}
      </div>
    );
  }
}

const mapStateToExperimentMetricsDropdownProps = (state) => {
  return {
    outcomeType: state.outcomeType,
    evaluationMetrics: state.evaluationMetrics,
    algorithms: state.algorithms,
    statuses: state.statuses
  };
};

const ConnectedExperimentsDropdown = connect(mapStateToExperimentMetricsDropdownProps)(ExperimentMetricsDropdown);

export default ConnectedExperimentsDropdown;
