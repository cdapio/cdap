import React from 'react';
import cloneDeep from 'lodash/cloneDeep'
import isEmpty from 'lodash/isEmpty';
import findIndex from 'lodash/findIndex';
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

require('./PropertySelector.scss');

class PropertySelector extends React.Component {
  currentPropertyIndex = 0;
  propertyMap;
  properties;
  constructor(props) {
    super(props);
    this.properties = [];
    this.state = {
      schemas: isEmpty(this.props.selectedSchemas) ? [] : cloneDeep(this.props.selectedSchemas),
      columnTypes: new Set(),
      filterKey: '',
      dropdownOpen: false,
      filterType: 'All',
    }
  }

  componentDidMount() {
    if (!isEmpty(this.props.selectedSchemas)) {
      this.state.schemas = isEmpty(this.props.selectedSchemas) ? [] : cloneDeep(this.props.selectedSchemas);
      this.state.schemas.map(schema => {
        schema.schemaColumns.map(column => {
          this.state.columnTypes.add(column.columnType);
        })
      })
    };
    console.log("Mounting Property Selector", Array.from(this.state.columnTypes));
  }

  handleColumnChange(schema, checkList) {
    let updateObj = {
      property: this.properties[this.currentPropertyIndex],
      schemaName: schema.schemaName,
      schemaColumns: schema.schemaColumns.filter((item, index) => checkList.get(index))
    }
    this.props.updatePropertyMap(this.getUpdatedPropertyMap(this.props.propertyMap, updateObj));
    this.render();
  }

  getUpdatedPropertyMap(prevPropertyMap, updateObj) {
    let propertyMap = cloneDeep(prevPropertyMap);
    let mappedProperty = propertyMap.get(updateObj.property);
    if (mappedProperty) {
      if(isEmpty(updateObj.schemaColumns)) {
        mappedProperty.delete(updateObj.schemaName);
      } else {
        mappedProperty.set(updateObj.schemaName, updateObj.schemaColumns);
      }
    } else if(!isEmpty(updateObj.schemaColumns)){
      propertyMap.set(updateObj.property, new Map([[updateObj.schemaName, updateObj.schemaColumns]]));
    }
    return propertyMap;
  }

  onAccordionChange(index) {
    this.currentPropertyIndex = index % this.properties.length;
    let propertyMap = this.props.propertyMap;
    let property = this.properties[this.currentPropertyIndex];
    let schemas = isEmpty(this.props.selectedSchemas) ? [] : cloneDeep(this.props.selectedSchemas);
    schemas.forEach((schema) => {
      if (propertyMap.has(property)) {
        let checkedCols = propertyMap.get(property).get(schema.schemaName);
        if(checkedCols) {
            schema.schemaColumns.map(column => {
              column.checked = findIndex(checkedCols, {columnName: column.columnName }) >= 0;
              return column;
            })
        }
      }
    });
    this.setState({
      schemas: schemas
    })
  }

  getColumns(propertyMap, property) {
    console
    let columns = [];
    if (propertyMap.has(property)) {
      propertyMap.get(property).forEach((value, key) => {
        value.map(column => {
          columns.push(key + ': ' + column.columnName)
        })
      })
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

  toggleDropDown(){
    this.setState(prevState => ({
      dropdownOpen: !prevState.dropdownOpen
    }));
  }

  onColumnTypeChange(type) {
    this.setState({
      filterType: type
    });
  }

  render() {
    let updatedPropMap = new Map();
    this.properties = this.props.availableProperties.map((property) => {
      if(isEmpty(property.subParams)){
        updatedPropMap.set(property.paramName, [{
          header: "none",
          isCollection: true,
          values: this.getColumns(this.props.propertyMap, property.paramName)
          }]);
      } else {
        let subParamValues = [];
        property.subParams.forEach(subParam => {
          subParamValues.push({
            header: subParam.paramName,
            isCollection: subParam.isCollection,
            values: this.getColumns(this.props.propertyMap, property.paramName, subParam.paramName)
          })
        })
      }
      return property.paramName;
    });

    return (
      <div className="property-step-container">
        <div className="property-container">
          <Accordion onChange={this.onAccordionChange.bind(this)}>
            {
              Array.from(updatedPropMap.keys()).map(property => {
                return (
                  <AccordionItem key={property}>
                    <AccordionItemTitle>
                      {property}
                    </AccordionItemTitle>
                    <AccordionItemBody>
                      {/* <List dataProvider={updatedPropMap.get(property)} /> */}
                    </AccordionItemBody>
                  </AccordionItem>
                )
              })
            }
          </Accordion>
        </div>
        <div className="schema-container">
          <div className="filter-container">
            <label>Column Selection</label>
            <Dropdown isOpen={this.state.dropdownOpen} toggle={this.toggleDropDown.bind(this)}>
              <DropdownToggle caret>
                {this.state.filterType}
            </DropdownToggle>
              <DropdownMenu>
                {
                  ["All"].concat(Array.from(this.state.columnTypes)).map((type) => {
                    return (
                      <DropdownItem  onClick={this.onColumnTypeChange.bind(this,type)}>{type}</DropdownItem>
                    )
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
                }).filter((item) => this.columnfilter(item, this.state.filterKey, this.state.filterType))
                return <div className="schema">
                  <CheckList dataProvider={columns}
                    title={schema.schemaName} handleChange={this.handleColumnChange.bind(this, schema)} />
                </div>
              })
            }
          </div>
        </div>
      </div>
    )
  }
}

export default PropertySelector;