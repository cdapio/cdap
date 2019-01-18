/* eslint react/prop-types: 0 */
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
