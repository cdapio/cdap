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
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import findIndex from 'lodash/findIndex';
import find from 'lodash/find';
import difference from 'lodash/difference';
import PropTypes from 'prop-types';

import CheckList from '../CheckList';
import { InputGroup, Input } from 'reactstrap';
import { Dropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';

import {
  Accordion,
  AccordionItem,
  AccordionItemTitle,
  AccordionItemBody,
} from 'react-accessible-accordion';

import 'react-accessible-accordion/dist/fancy-example.css';
import List from '../List';
import { getPropertyUpdateObj, updatePropertyMapWithObj, toCamelCase } from '../util';
import InfoTip from '../InfoTip';
import FEDataServiceApi from '../feDataService';
import NamespaceStore from 'services/NamespaceStore';

require('./PropertySelector.scss');

class PropertySelector extends React.Component {
  currentProperty = undefined;
  currentSubProperty = "none";
  propertyMap;
  lastSchemas;

  constructor(props) {
    super(props);
    this.state = {
      schemas: isEmpty(this.props.selectedSchemas) ? [] : cloneDeep(this.props.selectedSchemas),
      columnTypes: new Set(),
      filterKey: '',
      dropdownOpen: false,
      filterType: 'All',
      showAdvance: false,
    };
  }

  isSchemaChanged() {
    let detectedSchemas = this.props.detectedProperties.map(schema => schema.dataSchemaName);
    let currentSchemas = this.state.schemas.map(schema => schema.schemaName);
    let diff = difference(currentSchemas, detectedSchemas);
    return diff.length > 0;
  }

  componentDidMount() {
    if (this.isSchemaChanged()) {
      this.detectProperties();
    } else {
      this.setDefaultPropertyTobeConigured();
    }

  }

  setDefaultPropertyTobeConigured() {
    if (!isEmpty(this.props.availableProperties)) {
      this.currentProperty = find(this.props.availableProperties, {groupName : 'basic'});
      if (this.currentProperty) {
        this.onAccordionChange(this.currentProperty.paramName);
      }
    }
  }

  detectProperties() {
    FEDataServiceApi.detectProperties({
      namespace: NamespaceStore.getState().selectedNamespace
    },
      this.props.selectedSchemas.map(schema => schema.schemaName)
    ).subscribe(
      result => {
        this.setDetectedProperties(result);
        this.props.setDetectedProperties(result);
        setTimeout(() => {
          this.setDefaultPropertyTobeConigured();
        });
      }
    );
  }

  setDetectedProperties(result) {
    let detectedPropertyMap = {};
    result.forEach(schema => {
      schema.columnInfo.forEach(column => {
        if (!detectedPropertyMap.hasOwnProperty(column.columnType)) {
          detectedPropertyMap[column.columnType] = {};
        }
        if (!detectedPropertyMap[column.columnType].hasOwnProperty(schema.dataSchemaName)) {
          detectedPropertyMap[column.columnType][schema.dataSchemaName] = [];
        }
        detectedPropertyMap[column.columnType][schema.dataSchemaName].push(column.columnName);
      });
    });
    for (let propertyName in detectedPropertyMap) {
      let property = find(this.props.availableProperties, { paramName: propertyName });
      if (property) {
        let updatePropMap = cloneDeep(this.props.propertyMap);
        for (let schemaName in detectedPropertyMap[propertyName]) {
          let schema = find(this.state.schemas, { schemaName: schemaName });
          if (schema) {
            let schemaColumns = schema.schemaColumns.filter((item) => {
              return (detectedPropertyMap[propertyName][schemaName].indexOf(item.columnName) >= 0);
            }).map(column => {
              column.checked = true;
              return column;
            });

            let updateObj = getPropertyUpdateObj(property, "none", schema.schemaName, schemaColumns);
            updatePropertyMapWithObj(updatePropMap, updateObj);
            this.props.updatePropertyMap(updatePropMap);
          }
        }
      }
    }

  }

  handleColumnChange(schema, checkList) {
    if (this.currentProperty) {
      let schemaColumns = schema.schemaColumns.filter((item) => {
        if (checkList.get(item.columnName) == undefined) {
          return item.checked;
        } else {
          return checkList.get(item.columnName);
        }
      }).map(column => {
        column.checked = true;
        return column;
      });
      let updateObj = getPropertyUpdateObj(this.currentProperty, this.currentSubProperty, schema.schemaName, schemaColumns);
      let updatePropMap = cloneDeep(this.props.propertyMap);
      updatePropertyMapWithObj(updatePropMap, updateObj);
      this.props.updatePropertyMap(updatePropMap);
    }

    this.setState({
      schemas: this.getUpdateSchemas()
    });
  }



  onHeaderClick(property, subProperty) {
    this.currentSubProperty = subProperty;
    this.setState({
      schemas: this.getUpdateSchemas()
    });
  }

  getUpdateSchemas() {
    let propertyMap = this.props.propertyMap;
    let schemas = isEmpty(this.props.selectedSchemas) ? [] : cloneDeep(this.props.selectedSchemas);
    let checkedCols = this.getSchemaColumns(propertyMap, this.currentProperty.paramName, this.currentSubProperty);
    schemas.forEach((schema) => {
      if (propertyMap.has(this.currentProperty.paramName)) {
        if (checkedCols) {
          schema.schemaColumns.map(column => {
            column.checked = findIndex(checkedCols, { schema: schema.schemaName, column: column.columnName }) >= 0;
            return column;
          });
        }
      }
    });
    return schemas;
  }


  onAccordionChange(property) {
    this.currentProperty = find(this.props.availableProperties, {paramName: property});
    if (this.currentProperty) {
      if (isEmpty(this.currentProperty.subParams)) {
        this.currentSubProperty = "none";
      } else {
        this.currentSubProperty = this.currentProperty.subParams[0].paramName;
      }
      this.setState({
        schemas: this.getUpdateSchemas()
      });
    }
  }

  getSchemaColumns(propertyMap, propertyName, subPropertyName) {
    let columns = [];
    if (propertyMap.has(propertyName)) {
      let subSchemaMap = find(propertyMap.get(propertyName), { header: subPropertyName });
      if (subSchemaMap) {
        subSchemaMap.value.forEach((value, key) => {
          value.map(column => {
            columns.push({
              schema: key,
              column: column.columnName
            });
          });
        });
      }
    }
    return columns;
  }

  onFilterKeyChange(event) {
    this.setState({
      filterKey: event.target.value
    });
  }

  columnfilter(item, key, type) {
    if (type != "All") {
      if (item.columnType == type) {
        if (key != '') {
          return item.columnName.indexOf(key) > -1;
        }
      } else {
        return false;
      }
    } else {
      if (key != '') {
        return item.columnName.indexOf(key) > -1;
      }
    }
    return true;
  }

  toggleDropDown() {
    this.setState(prevState => ({
      dropdownOpen: !prevState.dropdownOpen
    }));
  }

  onColumnTypeChange(type) {
    this.setState({
      filterType: type
    });
  }

  isSingleSelect(propMap, subProperty) {
    if (propMap) {
      if (isEmpty(propMap.subParams)) {
        return !propMap.isCollection;
      } else {
        let subProp = find(propMap.subParams, { paramName: subProperty });
        if (subProp) {
          return !subProp.isCollection;
        }
      }
    }
    return false;
  }

  render() {
    let updatedPropMap;
    let basicPropMap = new Map();
    let advancePropMap = new Map();
    let propertyDisplayNameMap = new Map();
    if (isNil(this.currentProperty) && !isEmpty(this.props.availableProperties)) {
      this.currentProperty = find(this.props.availableProperties, {groupName : 'basic'});
    }
    this.props.availableProperties.map((property) => {
      if (property.groupName == 'basic') {
        updatedPropMap = basicPropMap;
      } else {
        updatedPropMap = advancePropMap;
      }
      propertyDisplayNameMap.set(property.paramName, isEmpty(property.displayName) ? property.paramName : property.displayName);
      if (isEmpty(property.subParams)) {
        updatedPropMap.set(property.paramName, [{
          header: "none",
          displayName: "none",
          isCollection: true,
          isSelected: false,
          isMandatory: property.isMandatory,
          description: property.description,
          values: this.getSchemaColumns(this.props.propertyMap, property.paramName, "none").map(obj => ({ parent: obj.schema, child: obj.column }))
        }]);
      } else {
        let subParamValues = [];
        property.subParams.forEach(subParam => {
          subParamValues.push({
            header: subParam.paramName,
            displayName: isEmpty(subParam.displayName) ? subParam.paramName : subParam.displayName,
            isCollection: subParam.isCollection,
            isMandatory: property.isMandatory,
            description: property.description,
            isSelected: this.currentProperty.paramName == property.paramName && this.currentSubProperty == subParam.paramName,
            values: this.getSchemaColumns(this.props.propertyMap, property.paramName, subParam.paramName).map(obj => ({ parent: obj.schema, child: obj.column }))
          });
        });
        updatedPropMap.set(property.paramName, subParamValues);
      }
    });

    return (
      <div className="property-step-container">
        <div className="property-container">
          <div className="config-selector-header">Property</div>
          <div className="config-container">
            <div className="config-header-label">Basic</div>
            <div className="config-item-container">
              <Accordion onChange={this.onAccordionChange.bind(this)}>
                {
                  Array.from(basicPropMap.keys()).map((property, index) => {
                    let isMandatory = false;
                    let subParams = basicPropMap.get(property);
                    let description;
                    if (!isEmpty(subParams)) {
                      isMandatory = subParams[0].isMandatory;
                      description = subParams[0].description;
                    }
                    return (
                      <AccordionItem key={property} uuid = {property} expanded={index == 0 ? true : false}>
                        <AccordionItemTitle className = { property == this.currentProperty.paramName ? "accordion__title selected" : "accordion__title"}>
                          <div className="title-items">
                            {
                              description && <InfoTip id={property + '_InfoTip'} description={description} />
                            }
                            {
                              isMandatory && <i className="fa fa-asterisk mandatory"></i>
                            }
                            <div className="heading" title={propertyDisplayNameMap.get(property)}>{propertyDisplayNameMap.get(property)}</div>
                          </div>
                          <div className="accordion__arrow" role="presentation" />
                        </AccordionItemTitle>

                        <AccordionItemBody>
                          {
                            basicPropMap.get(property).map(propValue => {
                              return (<List dataProvider={propValue.values}
                                key={(propValue.header == "none") ? property : (propValue.header + propValue.isSelected)}
                                header={(propValue.header == "none") ? undefined : propValue.displayName}
                                headerClass={propValue.isSelected ? "list-header-selected" : "list-header"}
                                onHeaderClick={this.onHeaderClick.bind(this, property, propValue.header)} />);
                            })
                          }
                        </AccordionItemBody>
                      </AccordionItem>
                    );
                  })
                }
              </Accordion>
            </div>
          </div>
          <div className="config-container">
            <div className="advance-control" onClick={() => { this.setState(prevState => ({ showAdvance: !prevState.showAdvance })); }}>
              <div className="config-header-label">Advance</div>
              <i className={this.state.showAdvance ? "fa fa-caret-up" : "fa fa-caret-down"}></i>
            </div>
            {
              this.state.showAdvance &&
              <div className="config-item-container">
                <Accordion onChange={this.onAccordionChange.bind(this)}>
                  {
                    Array.from(advancePropMap.keys()).map((property) => {
                      let isMandatory = false;
                      let subParams = advancePropMap.get(property);
                      let description;
                      if (!isEmpty(subParams)) {
                        isMandatory = subParams[0].isMandatory;
                        description = subParams[0].description;
                      }
                      return (
                        <AccordionItem key={property} uuid = {property}>
                          <AccordionItemTitle className = { property == this.currentProperty.paramName? "accordion__title selected" : "accordion__title"}>
                            <div className="title-items">
                              {
                                description && <InfoTip id={property + '_InfoTip'} description={description} />
                              }
                              {
                                isMandatory && <i className="fa fa-asterisk mandatory"></i>
                              }
                              <div className="heading" title={propertyDisplayNameMap.get(property)}>{propertyDisplayNameMap.get(property)}</div>
                            </div>
                            <div className="accordion__arrow" role="presentation" />
                          </AccordionItemTitle>

                          <AccordionItemBody>
                            {
                              advancePropMap.get(property).map(propValue => {
                                return (<List dataProvider={propValue.values}
                                  key={(propValue.header == "none") ? property : (propValue.header + propValue.isSelected)}
                                  header={(propValue.header == "none") ? undefined : propValue.displayName}
                                  headerClass={propValue.isSelected ? "list-header-selected" : "list-header"}
                                  onHeaderClick={this.onHeaderClick.bind(this, property, propValue.header)} />);
                              })
                            }
                          </AccordionItemBody>
                        </AccordionItem>
                      );
                    })
                  }
                </Accordion>
              </div>
            }
          </div>
        </div>
        <div className="schema-container">
          <div className="config-selector-header">{"Select dataset columns for : " + propertyDisplayNameMap.get(this.currentProperty.paramName)
            + (isEmpty(this.currentProperty.subParams) ? "" : (" (" + toCamelCase(this.currentSubProperty) + ")"))}</div>
          <div className="schema-filter-container">
            <label>Column Type</label>
            <Dropdown isOpen={this.state.dropdownOpen} toggle={this.toggleDropDown.bind(this)}>
              <DropdownToggle caret>
                {this.state.filterType}
              </DropdownToggle>
              <DropdownMenu>
                {
                  ["All"].concat(Array.from(this.state.columnTypes)).map((type) => {
                    return (
                      <DropdownItem key={type} onClick={this.onColumnTypeChange.bind(this, type)}>{type}</DropdownItem>
                    );
                  })
                }
              </DropdownMenu>
            </Dropdown>
            <div className="spacer"></div>
            <InputGroup>
              <Input placeholder="search" onChange={this.onFilterKeyChange.bind(this)} />
              <i className="search-icon fa fa-search"></i>
            </InputGroup>
          </div>
          <div className="schemas">
            {
              this.state.schemas.map(schema => {
                let columns = schema.schemaColumns.map(column => {
                  column.name = column.columnName;
                  return column;
                }).filter((item) => this.columnfilter(item, this.state.filterKey, this.state.filterType));
                return (<div className="schema" key={schema.schemaName}>
                  <CheckList dataProvider={columns} isSingleSelect={this.isSingleSelect(this.currentProperty, this.currentSubProperty)}
                    title={"Dataset: " + schema.schemaName} handleChange={this.handleColumnChange.bind(this, schema)} />
                </div>);
              })
            }
          </div>
        </div>
      </div>
    );
  }
}

export default PropertySelector;
PropertySelector.propTypes = {
  selectedSchemas: PropTypes.array,
  detectedProperties: PropTypes.array,
  availableProperties: PropTypes.array,
  setDetectedProperties: PropTypes.func,
  propertyMap: PropTypes.any,
  updatePropertyMap: PropTypes.func
};
