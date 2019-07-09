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
import cloneDeep from 'lodash/cloneDeep';
import PropTypes from 'prop-types';
import { Input } from 'reactstrap';
import { isNil, isEmpty, remove } from 'lodash';
import InfoTip from '../InfoTip';

require('./SinkSelector.scss');

class SinkSelector extends React.Component {
  availableSinks;
  sinkConfigurations;
  configMap;
  constructor(props) {
    super(props);
    this.configPropList = [];
    this.configMap = {};
    this.state = {
      sinks: []
    };
    this.onSinkChange = this.onSinkChange.bind(this);
    this.onValueUpdated = this.onValueUpdated.bind(this);
  }

  componentDidMount() {
    this.availableSinks = cloneDeep(this.props.availableSinks);
    if (!isEmpty(this.availableSinks)) {
      this.availableSinks.forEach(element => {
        if (isNil(this.configMap[element.paramName])) {
          this.configMap[element.paramName] = {};
        }
        if (!isEmpty(element.subParams)) {
          element.subParams.forEach(subElement => {
            if (!isEmpty(subElement.defaultValue)) {
              this.configMap[element.paramName][subElement.paramName] = subElement.defaultValue;
            }
          });
        }
      });
    }

    this.sinkConfigurations = cloneDeep(this.props.sinkConfigurations);
    if (!isNil(this.sinkConfigurations)) {
      let sinkList = [];
      for (let property in this.sinkConfigurations) {
        if (property) {
          if (isNil(this.configMap[property])) {
            this.configMap[property] = {};
          }
          this.configMap[property] = this.sinkConfigurations[property];
          sinkList.push(property);
        }
      }

      this.setState({
        sinks: sinkList
      });
    }
  }


  updateConfiguration() {
    const sinkConfigurations = {};
    if (!isEmpty(this.state.sinks) && this.state.sinks.length>0) {
      this.state.sinks.forEach(element => {
        sinkConfigurations[element] = {};
        if (!isNil(this.configMap[element])) {
          sinkConfigurations[element] = this.configMap[element];
        }
      });
    }
    this.sinkConfigurations = sinkConfigurations;
    console.log("Update store with Sink -> ", this.sinkConfigurations);
    this.props.setSinkConfigurations(this.sinkConfigurations);
  }

  onSinkChange(evt) {
    let sinkList = [...this.state.sinks];
    if (evt.target.checked) {
      sinkList.push(evt.target.value);
    } else {
      sinkList = remove(sinkList, function (item) {
        return item !== evt.target.value;
      });
    }
    this.setState({
      sinks: sinkList
    });
    setTimeout(()=>{
      this.updateConfiguration();
    });
  }

  onValueUpdated(parent, child, evt) {
    const value = evt.target.value;
    if (isNil(this.configMap[parent])) {
      this.configMap[parent] = {};
    }
    this.configMap[parent][child] = value;
    this.updateConfiguration();
  }

  render() {
    return (
      <div className='sink-step-container'>
        {
          (this.props.availableSinks).map((item) => {
            return (
              <div className="config-container" key={item.paramName}>
                <div className="config-header-label">
                  <Input
                    type="checkbox"
                    value={item.paramName}
                    onChange={this.onSinkChange}
                    checked={this.state.sinks.includes(item.paramName)}
                  />
                  <span>{item.displayName}</span>
                </div>

                {
                  this.state.sinks.includes(item.paramName) &&
                  <div className="config-item-container">
                    {
                      (item.subParams).map(param => {
                        return (
                          <div className='list-row' key={param.paramName}>
                            <div className='name'>{param.displayName}
                              {
                                param.isMandatory && <i className="fa fa-asterisk mandatory"></i>
                              }
                            </div>
                            <div className='colon'>:</div>
                            <Input className='value' type="text" name="value"
                              placeholder={'Enter ' + param.displayName + ' value'}
                              defaultValue={isNil(this.configMap[item.paramName][param.paramName]) ? '' : this.configMap[item.paramName][param.paramName]}
                              onChange={this.onValueUpdated.bind(this, item.paramName, param.paramName)} />
                            {
                              param.description &&
                              <InfoTip id={param.paramName + "_InfoTip"} description={param.description}></InfoTip>
                            }
                          </div>
                        );
                      })
                    }
                  </div>
                }
              </div>
            );
          })
        }
      </div>
    );
  }
}
export default SinkSelector;
SinkSelector.propTypes = {
  availableSinks: PropTypes.array,
  sinkConfigurations: PropTypes.any,
  setSinkConfigurations: PropTypes.func
};
