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
import PieChart from 'components/PieChart';
require('./PieChartWithLegend.scss');

export default function PieChartWithLegends({data, width, height}) {
  return (
    <div className="pie-chart-with-legends">
      <PieChart
        data={data}
        width={width}
        height={height}
      />
      <div className="pie-chart-legends">
        {
          !data.length ? null :
            data.map(d => {
              return (
                <div>
                  <span
                    className="pie-legend-color"
                    style={{backgroundColor: d.color}}
                  />
                  <span>{d.value}</span>
                </div>
              );
            })
        }
      </div>
    </div>
  );
}
PieChartWithLegends.defaultProps = {
  width: 150,
  height: 150
};
PieChartWithLegends.propTypes = {
  data: PropTypes.arrayOf(PropTypes.shape({
    color: PropTypes.string,
    value: PropTypes.string
  })),
  width: PropTypes.number,
  height: PropTypes.number
};
