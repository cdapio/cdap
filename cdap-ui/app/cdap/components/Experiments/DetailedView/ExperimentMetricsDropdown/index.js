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
import colorVariables from 'styles/variables.scss';

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
      value: 'RMSE',
      colorRange: [
        colorVariables.bluegrey01,
        colorVariables.bluegrey02,
        colorVariables.bluegrey03,
        colorVariables.bluegrey04,
        colorVariables.bluegrey05
      ]
    },
    {
      id: 'mae',
      value: 'Mean Avg Error',
      colorRange: [
        colorVariables.green01,
        colorVariables.green02,
        colorVariables.green03,
        colorVariables.green04,
        colorVariables.green05
      ]
    },
    {
      id: 'r2',
      value: 'R2',
      colorRange: [
        colorVariables.blue01,
        colorVariables.blue02,
        colorVariables.blue03,
        colorVariables.blue04,
        colorVariables.blue05
      ]
    },
    {
      id: 'evariance',
      value: 'Accuracy',
      colorRange: [
        colorVariables.orange01,
        colorVariables.orange02,
        colorVariables.orange03,
        colorVariables.orange04,
        colorVariables.orange05
      ]
    }
  ];
  static categoricalKeys = [
    {
      id: 'precision',
      value: 'Precision',
       colorRange: [
        colorVariables.blue01,
        colorVariables.blue02,
        colorVariables.blue03,
        colorVariables.blue04,
        colorVariables.blue05
      ]
    },
    {
      id: 'recall',
      value: 'Recall',
      colorRange: [
        colorVariables.orange01,
        colorVariables.orange02,
        colorVariables.orange03,
        colorVariables.orange04,
        colorVariables.orange05
      ]
    },
    {
      id: 'f1',
      value: 'F1',
      colorRange: [
        colorVariables.green01,
        colorVariables.green02,
        colorVariables.green03,
        colorVariables.green04,
        colorVariables.green05
      ]
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
  renderMetricBarChart = ({id: metric, value: label, colorRange}) => {
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
    let values = this.props.evaluationMetrics[metric] || {};
    let {width, height} = this.containerRef.getBoundingClientRect();
    return (
      <MetricChartWithLegend
        colorRange={colorRange}
        xAxisTitle={label}
        metric={metric}
        values={values.histo || []}
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
        {this.renderMetricBarChart(matchingKey)}
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
