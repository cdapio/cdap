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
import React from 'react';
import isEmpty from 'lodash/isEmpty';
import { Input } from 'reactstrap';
import InfoTip from '../InfoTip';

require('./NameValueList.scss');

class NameValueList extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      newName: '',
      newValue: '',
      showAdvance: false
    };
  }
  onNewNameChange(event) {
    this.setState({
      newName: event.target.value
    });
  }

  onNewValueChange(event) {
    this.setState({
      newValue: event.target.value
    });
  }

  onValueUpdated(item, event) {
    this.props.updateNameValue(item.itemIndex, {
      ...item,
      value: event.target.value
    });
  }

  onAdd() {
    if (!isEmpty(this.state.newName) && !isEmpty(this.state.newValue)) {
      this.props.addNameValue({
        name: this.state.newName,
        value: this.state.newValue,
        dataType: "string",
        isCollection: false
      });
      this.setState({
        newName: '',
        newValue: ''
      });
    }
  }

  render() {
    let listData = (isEmpty(this.props.dataProvider) ? [] : this.props.dataProvider).map((item,index) => {
      item.itemIndex = index;
      return item;
    });
    let basicData = this.props.dataProvider.filter(item => item.isMandatory);
    let advanceData = this.props.dataProvider.filter(item => !item.isMandatory);
    console.log("Rendering list ", listData);
    return (
      <div className="engine-config-container">
        {
          !isEmpty(basicData) &&
          <div className="config-container">
            <div className="config-header-label">Basic</div>
            <div className="config-item-container">
              {
                basicData.map((item) => {
                  return (
                    <div className='list-row' key={item.name}>
                      <div className='name'>{item.name}
                          {
                            item.isMandatory && <i className = "fa fa-asterisk mandatory"></i>
                          }
                       </div>
                      <div className='colon'>:</div>
                      <Input className='value' type="text" name="value" placeholder='value'
                        defaultValue={item.value} onChange={this.onValueUpdated.bind(this, item)} />
                      {
                        item.description &&
                        <InfoTip id = {item.name + "_InfoTip"} description = {item.description}></InfoTip>
                      }
                    </div>);
                })
              }
            </div>
          </div>
        }
        {
          !isEmpty(advanceData) &&
          <div className="config-container">
            <div className="advance-control" onClick={() => { this.setState(prevState => ({ showAdvance: !prevState.showAdvance })); }}>
              <div className="config-header-label">Advance</div>
              <i className={this.state.showAdvance ? "fa fa-caret-up" : "fa fa-caret-down"}></i>
            </div>
            {
              this.state.showAdvance &&
              <div className="config-item-container">
                {
                  advanceData.map((item) => {
                    return (
                      <div className='list-row' key={item.name}>
                        <div className='name'>{item.name}</div>
                        <div className='colon'>:</div>
                        <Input className='value' type="text" name="value" placeholder='value'
                          defaultValue={item.value} onChange={this.onValueUpdated.bind(this, item)} />
                        {
                          item.toolTip &&
                          <i className="fa fa-info-circle field-info" title={item.toolTip}></i>
                        }
                      </div>);
                  })
                }
              </div>
            }


          </div>
        }
      </div>
    );
  }
}
export default NameValueList;
