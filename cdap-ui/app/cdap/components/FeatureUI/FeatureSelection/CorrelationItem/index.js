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
import { isNil } from 'lodash';
import './CorrelationItem.scss';

class CorrelationItem extends Component {


  minValueChange = (evt) => {
    const result = { minValue: evt.target.value };
    this.props.changeItem(result, this.props.itemIndex);
  }

  maxValueChange = (evt) => {
    let result = { maxValue: "" };
    if (!isNil(evt)) {
      result = { maxValue: evt.target.value };
    }
    this.props.changeItem(result, this.props.itemIndex);
  }

  selectionChange = (event) => {
    let result = { enable: event.target.checked };
    this.props.changeItem(result, this.props.itemIndex);
  }


  render() {
    return (
      <div className="correlation-ietm-box">
        <input className="check-input" type="checkbox" onChange={this.selectionChange}/>{this.props.itemVO.name} :
        <input className="value-input" type="number" min="0" value={this.props.itemVO.minValue}
          onChange={this.minValueChange} disabled={!this.props.itemVO.enable}></input>
        {this.props.itemVO.doubleView ?
          <div>
            <label className="value-seperator">-</label>
            <input className="value-input" type="number" min="0" value={this.props.itemVO.maxValue}
              onChange={this.maxValueChange} disabled={!this.props.itemVO.enable}></input>
          </div>
          : null
        }
        {
          this.props.itemVO.hasRangeError ?
            <div className="error-box">Invalid range values</div>
            : null
        }
      </div>
    );
  }
}

export default CorrelationItem;
