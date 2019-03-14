
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

/* eslint react/prop-types: 0 */
import React, { Component } from 'react';
import { isNil } from 'lodash';
import './StatusItem.scss';
import {SUCCEEDED,DEPLOYED,FAILED,RUNNING} from '../../../config';



class StatusItem extends Component {

  constructor(props) {
    super(props);

    this.state = {className:this.getClassName(this.props.item),
      count:isNil(this.props.item.count) ? undefined : this.props.item.count,
      name:this.props.item.name};
  }

  itemClicked =() => {
    console.log("hello");
  }

  getClassName = (item) => {
    if (item.name === SUCCEEDED) {
      return "status-success";
    } else if (item.name === FAILED) {
      return "status-failed";
    } else if (item.name === RUNNING) {
      return "status-running";
    } else if (item.name === DEPLOYED) {
      return "status-deployed";
    }
  }

  render() {
    return (
      <div className={this.props.item.selected ? 'status-item-box box-selected': 'status-item-box box-un-selected' } onClick={this.props.itemClick}>
        <div className="header">
          <span className={this.state.className}></span>
          <span className="status-name">{this.state.name} </span>
          {
            isNil(this.state.count) ? null : <span>({this.state.count})</span>
          }
        </div>
      </div>
    );
  }
}

export default StatusItem;
