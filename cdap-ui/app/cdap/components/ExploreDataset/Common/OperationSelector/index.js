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
import Select from 'react-select';
import { isNil, isEmpty, remove, find, findIndex } from 'lodash';
import InfoTip from '../InfoTip';

require('./OperationSelector.scss');

class OperationSelector extends React.Component {
  availableOperations;
  operationConfigurations;
  operationMap;
  columns;
  constructor(props) {
    super(props);
    this.configPropList = [];
    this.operationMap = {};
    this.columns = [];
    this.state = {
      operations: [],
      selectedOptionMap: {}
    };
    this.onOperationChange = this.onOperationChange.bind(this);
    this.onValueUpdated = this.onValueUpdated.bind(this);
  }

  componentDidMount() {
    if (this.props.schema) {
      this.columns = this.props.schema.schemaColumns.map(column => {
        const value = column.columnName;
        const label = column.columnName;
        const type = column.columnType;
        return { value, label, type };
      });
    }
    this.availableOperations = cloneDeep(this.props.availableOperations);
    if (!isEmpty(this.availableOperations)) {
      this.availableOperations.forEach(element => {
        if (isNil(this.operationMap[element.paramName])) {
          this.operationMap[element.paramName] = {};
        }
        if (!isEmpty(element.subParams)) {
          element.subParams.forEach(subElement => {
            if (subElement.isSchemaSpecific && subElement.isCollection) {
              this.operationMap[element.paramName][subElement.paramName]
                = this.columns.filter((column) => this.columnFilter(column, subElement.dataType))
                  .reduce((result, item) => {
                    if (isEmpty(result)) {
                      result = item.value;
                    } else {
                      result += ("," + item.value);
                    }
                    return result;
                  }, "");
            }
            if (!isEmpty(subElement.defaultValue)) {
              this.operationMap[element.paramName][subElement.paramName] = subElement.defaultValue;
            }
          });
        }
      });
    }
    let operationList = [];
    this.operationConfigurations = cloneDeep(this.props.operationConfigurations);
    if (!isNil(this.operationConfigurations)) {
      for (let property in this.operationConfigurations) {
        if (property) {
          if (isNil(this.operationMap[property])) {
            this.operationMap[property] = {};
          }
          this.operationMap[property] = this.operationConfigurations[property];
          operationList.push(property);
        }
      }
    }

    this.setState({
      operations: operationList,
      selectedOptionMap: this.initSelectedOptionMap()
    });

    setTimeout(() => {
      this.updateConfiguration();
    });
  }


  updateConfiguration() {
    const operationConfigurations = {};
    if (!isEmpty(this.state.operations) && this.state.operations.length > 0) {
      this.state.operations.forEach(element => {
        operationConfigurations[element] = {};
        if (!isNil(this.operationMap[element])) {
          operationConfigurations[element] = this.operationMap[element];
        }
      });
    }
    this.operationConfigurations = operationConfigurations;
    this.props.updateOperationConfigurations(this.operationConfigurations);
  }

  onOperationChange(evt) {
    let operationList = [...this.state.operations];
    if (evt.target.checked) {
      operationList.push(evt.target.value);
    } else {
      operationList = remove(operationList, function (item) {
        return item !== evt.target.value;
      });
    }
    this.setState({
      operations: operationList
    });
    setTimeout(() => {
      this.updateConfiguration();
    });
  }

  onValueUpdated(parent, child, evt) {
    const value = evt.target.value;
    if (isNil(this.operationMap[parent])) {
      this.operationMap[parent] = {};
    }
    this.operationMap[parent][child] = value;
    this.updateConfiguration();
  }

  initSelectedOptionMap() {
    const selectedOptionMap = {};
    let selectedValue;
    (this.props.availableOperations).map((item) => {
      (item.subParams).map(param => {
        if (param.isSchemaSpecific) {
          selectedValue = this.operationMap[item.paramName][param.paramName];
          if (!isEmpty(selectedValue)) {
            if (param.isCollection) {
              selectedOptionMap[item.paramName + ":" + param.paramName] = selectedValue.split(",").map(value => find(this.columns, { "value": value }));
            } else {
              selectedOptionMap[item.paramName + ":" + param.paramName] = find(this.columns, { "value": selectedValue });
            }
          }
        }
      });
    });
    return selectedOptionMap;
  }

  handleSelectChange(parent, child, selectedOption) {
    const selectedOptionMap = { ...this.state.selectedOptionMap };
    selectedOptionMap[parent + ":" + child] = selectedOption;
    if (isNil(this.operationMap[parent])) {
      this.operationMap[parent] = {};
    }
    if (Array.isArray(selectedOption)) {
      this.operationMap[parent][child] = selectedOption.reduce((result, item) => {
        if (isEmpty(result)) {
          result = item.value;
        } else {
          result += ("," + item.value);
        }
        return result;
      }, "");
    } else {
      this.operationMap[parent][child] = isNil(selectedOption) ? "" : selectedOption.value;
    }
    this.updateConfiguration();
    this.setState({
      selectedOptionMap: selectedOptionMap
    });
  }

  columnFilter(column, dataType) {
    switch (dataType) {
      case "string":
        return column.type == "string";
      case "number":
        return findIndex(["int", "float", "number", "double", "short"], column.type) >= 0;
      default:
        return true;
    }
  }

  render() {
    return (
      <div className='operation-step-container'>
        {
          (this.props.availableOperations).map((item) => {
            return (
              <div className="config-container" key={item.paramName}>
                <div className="config-header-label">
                  <Input
                    type="checkbox"
                    value={item.paramName}
                    onChange={this.onOperationChange}
                    checked={this.state.operations.includes(item.paramName)}
                  />
                  <span>{item.displayName}</span>
                </div>

                {
                  this.state.operations.includes(item.paramName) &&
                  <div className="config-item-container">
                    {
                      (item.subParams).map(param => {
                        const subParamKey = `${item.paramName}:${param.paramName}`;
                        if (param.isSchemaSpecific && param.isCollection) {
                          return (
                            <div className='list-row' key={param.paramName}>
                              <div className='name'>{param.displayName}
                                {
                                  param.isMandatory && <i className="fa fa-asterisk mandatory"></i>
                                }
                              </div>
                              <div className='colon'>:</div>
                              <Select className='select-value' isMulti={true}
                                options={this.columns} onChange={this.handleSelectChange.bind(this, item.paramName, param.paramName)}
                                value={isNil(this.state.selectedOptionMap[subParamKey]) ? [] : this.state.selectedOptionMap[subParamKey]}
                              />
                              {
                                param.description &&
                                <InfoTip id={param.paramName + "_InfoTip"} description={param.description}></InfoTip>
                              }
                            </div>
                          );
                        } else if (param.isSchemaSpecific && !param.isCollection) {
                          return (
                            <div className='list-row' key={param.paramName}>
                              <div className='name'>{param.displayName}
                                {
                                  param.isMandatory && <i className="fa fa-asterisk mandatory"></i>
                                }
                              </div>
                              <div className='colon'>:</div>
                              <Select className='select-value'
                                value={isNil(this.state.selectedOptionMap[subParamKey]) ? [] : this.state.selectedOptionMap[subParamKey]}
                                options={this.columns} onChange={this.handleSelectChange.bind(this, item.paramName, param.paramName)} />
                              {
                                param.description &&
                                <InfoTip id={param.paramName + "_InfoTip"} description={param.description}></InfoTip>
                              }
                            </div>
                          );
                        } else {
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
                                defaultValue={(isNil(this.operationMap[item.paramName]) || isNil(this.operationMap[item.paramName][param.paramName])) ? '' : this.operationMap[item.paramName][param.paramName]}
                                onChange={this.onValueUpdated.bind(this, item.paramName, param.paramName)} />
                              {
                                param.description &&
                                <InfoTip id={param.paramName + "_InfoTip"} description={param.description}></InfoTip>
                              }
                            </div>
                          );
                        }
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
export default OperationSelector;
OperationSelector.propTypes = {
  availableOperations: PropTypes.array,
  operationConfigurations: PropTypes.any,
  updateOperationConfigurations: PropTypes.func,
  schema: PropTypes.any,
};
