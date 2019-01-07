/* eslint react/prop-types: 0 */
import React, { Component } from 'react';
import { Dropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import FilterItem from '../FilterItem/index';
import './FilterContainer.scss';
import { cloneDeep } from "lodash";
// import { TabContent, TabPane, Nav, NavItem, NavLink, Card, Button, CardTitle, CardText, Row, Col } from 'reactstrap';

class FilterContainer extends Component {
  filterTypeList = [{ id: 1, name: 'TopN' }, { id: 2, name: 'LowN' }, { id: 3, name: 'Range' }]
  filterColumnList = [{ id: 1, name: 'max' }, { id: 2, name: 'min' }, { id: 3, name: 'percentile' }, { id: 4, name: 'variance' }]
  filterViewMaps = [{ name: 'TopN', view: 'single' }, { name: 'LowN', view: 'single' }, { name: 'Range', view: 'double' }]

  constructor(props) {
    super(props);
    this.filterColumnList = this.props.filterColumns;
    //   this.updateIntialState();
    this.state = {
      orderbyOpen: false,
      selectedOrderbyColumn: { id: -1, name: 'Select' },
      orderByCOlumnList: cloneDeep(this.filterColumnList),
      filterItemList: [this.getFilterItemVO()],
      minLimitValue:"",
      maxLimitValue:""
    };
  }


  toggleOrderbyDropDown = () => {
    this.setState(prevState => ({
      orderbyOpen: !prevState.orderbyOpen
    }));
  }

  orderbyColumnChange = (item) => {
    this.setState({ selectedOrderbyColumn: item });
  }

  addFilterItem = () => {
    const filterItems = [...this.state.filterItemList];
    filterItems.push(this.getFilterItemVO());
    this.setState({ filterItemList: filterItems });
  }

  getFilterItemVO = () => {
    return {
      filterTypeList: this.filterTypeList,
      selectedFilterType: { id: -1, name: 'Select' },
      filterColumnList: this.filterColumnList,
      selectedFilterColumn: { id: -1, name: 'Select' },
      filterViewMaps: this.filterViewMaps,
      minValue: '', maxValue: ''
    };
  }

  changFilterItem = (value, index) => {
    const itemList = [...this.state.filterItemList];
    const item = itemList[index];
    if (value.hasOwnProperty('selectedFilterType')) {
      item['selectedFilterType'] = value.selectedFilterType;
    }

    if (value.hasOwnProperty('selectedFilterColumn')) {
      item['selectedFilterColumn'] = value.selectedFilterColumn;
    }

    if (value.hasOwnProperty('minValue')) {
      item['minValue'] = value.minValue;
    }

    if (value.hasOwnProperty('maxValue')) {
      item['maxValue'] = value.maxValue;
    }


    this.setState({ filterItemList: itemList });
  }

  removeFilterItem = (index) => {
    const itemList = [...this.state.filterItemList];
    if (itemList.length > 1) {
      itemList.splice(index, 1);
    }
    this.setState({ filterItemList: itemList });
  }

  minLimitChanged = (evt)=> {
    this.setState({minLimitValue: evt.target.value});
  }

  maxLimitChanged = (evt)=> {
    this.setState({maxLimitValue: evt.target.value});
  }

  applyFilter = () => {
    this.props.applyFilter(this.state);
  }



  render() {
    let filterItems = (
      <div className="filter-item-Container">
        {
          this.state.filterItemList.map((item, index) => {
            return (<FilterItem
              itemVO={item}
              itemIndex={index}
              changFilterItem={this.changFilterItem.bind(this)}
              removeFilterItem={this.removeFilterItem.bind(this, index)}
              key={'fi_' + index.toString()}>
            </FilterItem>);
          })
        }
      </div>
    );

    return (
      <div className="filter-container">
        <h3>Filters</h3>
        <div className="orderby-box">
          <label className="orderby-label">Orderby: </label>
          <Dropdown isOpen={this.state.orderbyOpen} toggle={this.toggleOrderbyDropDown}>
            <DropdownToggle caret>
              {this.state.selectedOrderbyColumn.name}
            </DropdownToggle>
            <DropdownMenu>
              {
                this.state.orderByCOlumnList.map((column) => {
                  return (
                    <DropdownItem onClick={this.orderbyColumnChange.bind(this, column)}
                      key={'orderby_' + column.id.toString()}
                    >{column.name}</DropdownItem>
                  );
                })
              }
            </DropdownMenu>
          </Dropdown>

        </div>
        {filterItems}
        <div className="control-box">
          <button onClick={this.addFilterItem}>+Add</button>
          <button onClick={this.applyFilter}>Apply</button>

        </div>
        <div className="limit-box">
          <label className="limit-label">Limit Within:   </label>
          <input className="limit-input" type="number" min="0" value={this.state.minLimitValue}
            onChange={this.minLimitChanged}></input>
          <label className="value-seperator">-</label>
          <input className="limit-input" type="number" min="0" value={this.state.maxLimitValue}
            onChange={this.maxLimitChanged}></input>
        </div>
      </div>
    );

  }

  removeFilterItem = (index) => {
    const itemList = [...this.state.filterItemList];
    if (itemList.length > 1) {
      itemList.splice(index, 1);
    }
    this.setState({ filterItemList: itemList });
  }
}

export default FilterContainer;
