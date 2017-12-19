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
import VegaLiteChart from 'components/VegaLiteChart';

var chartSpec = {
  "data": {
    "values": []
  },
  "mark": "bar",
  "encoding": {
    "x": {
      "field": "bin",
      "type": "nominal",
      "axis": {
          "title": null,
          "values": []
      }
    },
    "y": {
      "field": "count",
      "type": "quantitative",
      "axis": {
          "title": "#Of Models"
      }
    },
    "color": {
        "field": "bin",
        "type": "ordinal",
        "legend": {
            "title": null
        }
    }
  },
  "config": {
    "legend": {
      "labelFontSize": 13
    }
  }
};

export default function MetricChartWithLegend({xAxisTitle, values, height, width}) {
  let spec = {
    ...chartSpec,
    height,
    width
  };
  xAxisTitle ? spec.encoding.x.axis.title = xAxisTitle : delete spec.encoding.x.axis.title;
  return (
    <VegaLiteChart
      spec={spec}
      data={values}
      widthOffset={280}
      heightOffset={30}
    />
  );
}

MetricChartWithLegend.propTypes = {
  xAxisTitle: PropTypes.string,
  values: PropTypes.arrayOf(PropTypes.object),
  height: PropTypes.number,
  width: PropTypes.number
};
