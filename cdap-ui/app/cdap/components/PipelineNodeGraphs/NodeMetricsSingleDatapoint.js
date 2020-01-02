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
import isNil from 'lodash/isNil';
import T from 'i18n-react';
import { objectQuery } from 'services/helpers';

export default function NodeMetricsSingleDatapoint({ data }) {
  /*
    We would need this check
     - if we have only aggregated metric
     - or if we have a time series with only one data point.

     The aggregated metric would be just plain number and the single datapoint timeseries will be d[0].y
  */
  const renderData = (d) => {
    if (typeof d === 'object') {
      return objectQuery(d, 0, 'y') || '0';
    }
    return d;
  };
  if (!Array.isArray(data) && typeof data === 'object') {
    return (
      <div className="node-metrics-single-datapoint">
        {Object.keys(data).map((key) => {
          return (
            <span>
              <small>{data[key].label}</small>
              <span>
                {isNil(data[key].data)
                  ? T.translate('commons.notAvailable')
                  : renderData(data[key].data)}
              </span>
            </span>
          );
        })}
      </div>
    );
  }
  return <div className="node-metrics-single-datapoint">{data}</div>;
}
NodeMetricsSingleDatapoint.propTypes = {
  data: PropTypes.arrayOf(PropTypes.object),
};
