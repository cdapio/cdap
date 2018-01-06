/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import GroupedBarChart from 'components/GroupedBarChart';

const DEFAULT_DEPLOYED_COLOR = '#B9C0D8';
const DEFAULT_TOTAL_COLOR = '#5B6787';
const customEncoding = {
  "color": {
    "field": "type",
    "type": "nominal",
    "scale": {
      "range": [DEFAULT_DEPLOYED_COLOR, DEFAULT_TOTAL_COLOR]
      }
  },
  "column": {
    "field": "name", "type": "ordinal",
    "header": {"title": null}
  },
  "x": {
    "field": "type",
    "type": "nominal",
    "axis": {
      "labels": false,
      "title": null
    }
  },
  "y": {
    "field": "count",
    "type": "quantitative",
    "axis": {
      "title": "Models",
      "grid": false
    }
  }
};
export default function ExperimentsListBarChart({data}) {
  return (
    <div className="experiment-list-chart">
      <h5>Models Created and Deployed </h5>
      <GroupedBarChart
        data={data}
        customEncoding={customEncoding}
        width={(dimension, data) => ((dimension.width - 190) / (data.length / 2))}
        heightOffset={70}
      />
    </div>
  );
}

ExperimentsListBarChart.propTypes = {
  data: PropTypes.arrayOf(PropTypes.shape({
    name: PropTypes.string,
    type: PropTypes.string
  })).isRequired
};
