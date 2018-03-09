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
import React from 'react';
import * as d3Lib from 'd3';
import PieChartWithLegends from 'components/PieChartWithLegend';
import EmptyMetricMessage from 'components/Experiments/DetailedView/ExperimentMetricsDropdown/EmptyMetricMessage';

const HEIGHT_OF_PIE_CHART = 190;
const colorScale = d3Lib.scale.category20();
const ModelStatusesDistribution = ({modelStatuses}) => {
  if (!modelStatuses.length) {
    return (
      <EmptyMetricMessage
        mainMessage={`Model Status Distribution Unavailable`}
        popoverMessage={`Atleast one model has to be trained to get Model Status distribution`}
      />
    );
  }
  let statuses = modelStatuses.map(status => {
    return {
      ...status,
      value: status.bin,
      color: colorScale(status.bin)
    };
  });
  return (
    <PieChartWithLegends
      data={statuses}
      width={HEIGHT_OF_PIE_CHART}
      height={HEIGHT_OF_PIE_CHART}
    />
  );
};

ModelStatusesDistribution.propTypes = {
  modelStatuses: PropTypes.arrayOf(PropTypes.shape({
    bin: PropTypes.string,
    count: PropTypes.number
  }))
};

export default ModelStatusesDistribution;
