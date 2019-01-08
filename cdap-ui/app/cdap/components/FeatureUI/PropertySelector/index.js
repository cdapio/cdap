/* eslint react/prop-types: 0 */
import React from 'react';
import cloneDeep from 'lodash/cloneDeep';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import findIndex from 'lodash/findIndex';
import find from 'lodash/find';
import CheckList from '../CheckList';
import { Input } from 'reactstrap';
import { Dropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';

import {
  Accordion,
  AccordionItem,
  AccordionItemTitle,
  AccordionItemBody,
} from 'react-accessible-accordion';

// Demo styles, see 'Styles' section below for some notes on use.
import 'react-accessible-accordion/dist/fancy-example.css';
import List from '../List';
import { getPropertyUpdateObj, updatePropertyMapWithObj } from '../util';
require('./PropertySelector.scss');

class PropertySelector extends React.Component {
  currentPropertyIndex = 0;
  currentProperty = undefined;
  currentSubProperty = "none";
  propertyMap;

  constructor(props) {
    super(props);
    this.state = {
      schemas: isEmpty(this.props.selectedSchemas) ? [] : cloneDeep(this.props.selectedSchemas),
      columnTypes: new Set(),
      filterKey: '',
      dropdownOpen: false,
      filterType: 'All',
    };
  }

  componentDidMount() {
    if (!isEmpty(this.props.selectedSchemas)) {
      let schemas = isEmpty(this.props.selectedSchemas) ? [] : cloneDeep(this.props.selectedSchemas);
      schemas.map(schema => {
        schema.schemaColumns.map(column => {
          this.state.columnTypes.add(column.columnType);
        });
      });
      this.setState({
        schemas: schemas
      });
    }
  }

  handleColumnChange(schema, checkList) {
    if (this.currentProperty) {
      let schemaColumns = schema.schemaColumns.filter((item, index) => checkList.get(index)).map(column => {
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

  onAccordionChange(index) {
    this.currentPropertyIndex = index % this.props.availableProperties.length;
    this.currentProperty = this.props.availableProperties[this.currentPropertyIndex];
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
    let updatedPropMap = new Map();
    if (isNil(this.currentProperty) && !isEmpty(this.props.availableProperties)) {
      this.currentProperty = this.props.availableProperties[this.currentPropertyIndex];
    }
    this.props.availableProperties.map((property) => {
      if (isEmpty(property.subParams)) {
        updatedPropMap.set(property.paramName, [{
          header: "none",
          isCollection: true,
          isSelected: false,
          isMandatory: property.isMandatory,
          values: this.getSchemaColumns(this.props.propertyMap, property.paramName, "none").map(obj => ({parent: obj.schema, child: obj.column}))
        }]);
      } else {
        let subParamValues = [];
        property.subParams.forEach(subParam => {
          subParamValues.push({
            header: subParam.paramName,
            isCollection: subParam.isCollection,
            isMandatory: property.isMandatory,
            isSelected: this.currentProperty.paramName == property.paramName && this.currentSubProperty == subParam.paramName,
            values: this.getSchemaColumns(this.props.propertyMap, property.paramName, subParam.paramName).map(obj => ({parent: obj.schema, child: obj.column}))
          });
        });
        updatedPropMap.set(property.paramName, subParamValues);
      }
    });

    return (
      <div className="property-step-container">
        <div className="property-container">
          <Accordion onChange={this.onAccordionChange.bind(this)}>
            {
              Array.from(updatedPropMap.keys()).map(property => {
                let isMandatory = false;
                let subParams = updatedPropMap.get(property);
                if (!isEmpty(subParams)) {
                  isMandatory = subParams[0].isMandatory;
                }
                return (
                  <AccordionItem key={property}>
                    <AccordionItemTitle>
                      {property + (isMandatory ? "*" : "")}
                    </AccordionItemTitle>
                    <AccordionItemBody>
                      {
                        updatedPropMap.get(property).map(propValue => {
                          return (<List dataProvider={propValue.values}
                            key={(propValue.header == "none") ? property : (propValue.header + propValue.isSelected)}
                            header={(propValue.header == "none") ? undefined : (propValue.header + "*")}
                            headerClass={propValue.isSelected ? "list-header-selected" : "list-header"}
                            onHeaderClick={this.onHeaderClick.bind(this, property, propValue.header)} />);
                        })
                      }

                      {/* <List dataProvider={updatedPropMap.get(property)} /> */}
                    </AccordionItemBody>
                  </AccordionItem>
                );
              })
            }
          </Accordion>
        </div>
        <div className="schema-container">
          <div className="schema-filter-container">
            <label>Column Selection</label>
            <Dropdown isOpen={this.state.dropdownOpen} toggle={this.toggleDropDown.bind(this)}>
              <DropdownToggle caret>
                {this.state.filterType}
              </DropdownToggle>
              <DropdownMenu>
                {
                  ["All"].concat(Array.from(this.state.columnTypes)).map((type) => {
                    return (
                      <DropdownItem key = {type} onClick={this.onColumnTypeChange.bind(this, type)}>{type}</DropdownItem>
                    );
                  })
                }
              </DropdownMenu>
            </Dropdown>
            <Input placeholder="search" onChange={this.onFilterKeyChange.bind(this)} />
          </div>
          <div className="schemas">
            {
              this.state.schemas.map(schema => {
                let columns = schema.schemaColumns.map(column => {
                  column.name = column.columnName;
                  return column;
                }).filter((item) => this.columnfilter(item, this.state.filterKey, this.state.filterType));
                return (<div className="schema" key = {schema.schemaName}>
                  <CheckList dataProvider={columns} isSingleSelect={this.isSingleSelect(this.currentProperty, this.currentSubProperty)}
                    title={schema.schemaName} handleChange={this.handleColumnChange.bind(this, schema)} />
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
