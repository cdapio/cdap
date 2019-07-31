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

import React from 'react';
import PropTypes from 'prop-types';
import isNil from 'lodash/isNil';


require('./CorrelationRenderer.scss');

class CorrelationRenderer extends React.Component {
  constructor(props) {
    super(props);
  }
  refresh() {
    return true;
  }

  render() {
    const width = 400;
    const height = 27;
    const scaleHeight = 1;
    const tickWidth = 2;
    const barHeight = 8;
    const baseTickHeight = 15;
    const boundaryTickHeight = 10;
    const vo = this.props.value;
    if (isNil(vo)) {
      return "";
    }
    let value;
    let min;
    let max;
    let location;
    const viewBoxRect = "0 0 " + width + " " + height;
    if (vo.hasOwnProperty('value') && vo.hasOwnProperty('min') && vo.hasOwnProperty('max')) {
      value = vo['value'];
      min = vo['min'];
      max = vo['max'];
    }

    if (isNil(value)) {
      return "";
    } else {
      if (min < 0 ) {
        location = (value / max ) * (width / 2);
        return (
          <svg className = "corrationSvg" viewBox = {viewBoxRect} preserveAspectRatio="none">
            <rect x = "0" y = { height/2 - scaleHeight} width = { width }
                  height = { scaleHeight }  className="baseBar" />
            <rect x = "0" y = { height/2 - boundaryTickHeight/2}  width = { tickWidth }
                  height = { boundaryTickHeight } className="baseBar" />
            <rect x = { width/2 } y = { height/2 - baseTickHeight/2} width = { tickWidth }
                  height = { baseTickHeight } className = "baseBar" />
            <rect x = { width - tickWidth} y = { height/2 - boundaryTickHeight/2} width = { tickWidth }
                  height = { boundaryTickHeight } className="baseBar" />
            <rect x = { (location < 0) ? location + (width/2) : 0 } y = { height/2 - barHeight/2} width = { (location < 0)? (location * -1) : 0 }
                  height = { barHeight } className="negativeBar" />
            <rect x = { width/2 } y = { height/2 - barHeight/2 } width = { (location > 0)? location : 0 }
                  height = { barHeight } className="positiveBar" />
          </svg>
        );
      } else {
        location = (value / max ) * width;
        return (
          <svg viewBox = { viewBoxRect } preserveAspectRatio="none">
             <rect x = "0" y = { height/2 - scaleHeight} width = { width }
                  height = { scaleHeight }  className="baseBar" />
            <rect x = "0" y = { height/2 - baseTickHeight/2} width = { tickWidth }
                  height = { baseTickHeight } className = "baseBar" />
            <rect x = { width - tickWidth} y = { height/2 - boundaryTickHeight/2} width = { tickWidth }
                  height = { boundaryTickHeight } className="baseBar" />
            <rect x = "0" y = { height/2 - barHeight/2 } width = { location }
                  height = { barHeight } className="valueBar" />
          </svg>
        );
      }
    }

  }
}
export default CorrelationRenderer;
CorrelationRenderer.propTypes = {
  value: PropTypes.object
};
