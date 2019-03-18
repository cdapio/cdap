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
import { Dropdown, DropdownToggle, DropdownMenu, DropdownItem, Button, ButtonGroup } from 'reactstrap';
import FilterItem from '../FilterItem/index';
import './FilterContainer.scss';
import { cloneDeep } from "lodash";
import ToggleSwitch from 'components/ToggleSwitch';
import PropTypes from 'prop-types';

class FilterContainer extends Component {
  filterTypeList = [{ id: 1, name: 'TopN' }, { id: 2, name: 'LowN' }, { id: 3, name: 'Range' }]
  filterColumnList = [{ id: 1, name: 'max' }, { id: 2, name: 'min' }, { id: 3, name: 'percentile' }, { id: 4, name: 'variance' }]
  filterViewMaps = [{ name: 'TopN', view: 'single' }, { name: 'LowN', view: 'single' }, { name: 'Range', view: 'double' }]

  constructor(props) {
    super(props);
    this.filterColumnList = this.props.filterColumns;
    this.state = {
      orderbyOpen: false,
      selectedOrderbyColumn: { id: -1, name: 'Select' },
      selectedCompositeOption: "OR",
      isSelectCompositeOption: false,
      orderByCOlumnList: cloneDeep(this.filterColumnList),
      filterItemList: [this.getFilterItemVO()],
      minLimitValue: "0",
      maxLimitValue: "1000",
      activeApplyBtn: false,
      hasLimitError: false,
      limitErrorMsg: ""
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

  compositeOptionChange = (option) => {
    this.setState({ selectedCompositeOption: option });
  }

  switchChange = () => {
    const switchActiveStatus = !this.state.isSelectCompositeOption;
    this.setState({ selectedCompositeOption: "OR" });
    this.setState({ isSelectCompositeOption: switchActiveStatus });
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
      minValue: '', maxValue: '', hasRangeError: false,
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
      item['minValue'] = value.minValue.trim();
    }

    if (value.hasOwnProperty('maxValue')) {
      item['maxValue'] = value.maxValue.trim();
    }

    if (item.selectedFilterType.name === 'Range' && item.minValue != '' && item.maxValue != '') {
      const min = Number(item.minValue);
      const max = Number(item.maxValue);
      if (isNaN(item.minValue) || isNaN(item.maxValue) || max <= min) {
        item.hasRangeError = true;
      } else {
        item.hasRangeError = false;
      }
    } else {
      item.hasRangeError = false;
    }

    this.setState({ filterItemList: itemList });
    setTimeout(() => {
      this.updateApplyBtnStatus();
    }, 500);
  }

  removeFilterItem = (index) => {
    const itemList = [...this.state.filterItemList];
    if (itemList.length > 1) {
      itemList.splice(index, 1);
    }
    this.setState({ filterItemList: itemList });
    setTimeout(() => {
      this.updateApplyBtnStatus();
    }, 500);

  }

  minLimitChanged = (evt) => {
    const min = evt.target.value.trim();
    this.updateLimitError(min, this.state.maxLimitValue);
    this.setState({ minLimitValue: min });

    setTimeout(() => {
      this.updateApplyBtnStatus();
    }, 500);
  }

  maxLimitChanged = (evt) => {
    const max = evt.target.value.trim();
    this.updateLimitError(this.state.minLimitValue, max);
    this.setState({ maxLimitValue: max });

    setTimeout(() => {
      this.updateApplyBtnStatus();
    }, 500);

  }

  updateLimitError = (min, max) => {
    if (min === "" || max === "") {
      this.setState({ hasLimitError: true, limitErrorMsg: 'Limit values are required' });
    } else {
      const minValue = Number(min);
      const maxValue = Number(max);
      if (isNaN(min) || isNaN(max) || maxValue <= minValue) {
        this.setState({ hasLimitError: true, limitErrorMsg: 'Invalid limit range values' });
      } else {
        this.setState({ hasLimitError: false, limitErrorMsg: '' });
      }
    }
  }


  updateApplyBtnStatus = () => {
    let isValidFilterItems = true;

    for (let i = 0; i < this.state.filterItemList.length; i++) {
      const item = this.state.filterItemList[i];
      if (item.hasRangeError || item.selectedFilterType.id === -1 || item.selectedFilterColumn.id === -1) {
        isValidFilterItems = false;
      } else {
        const filterType = item.selectedFilterType.name;
        const mapItemList = this.filterViewMaps.filter((item) => item.name === filterType);
        const mapItem = mapItemList.length > 0 ? mapItemList[0] : {};
        if (mapItem.view === 'single') {
          isValidFilterItems = item.minValue != '';
        } else {
          isValidFilterItems = item.minValue != '' && item.maxValue != '';
        }
      }
    }

    // check limit range
    if (this.state.hasLimitError || !isValidFilterItems) {
      this.setState({ activeApplyBtn: false });
    } else {
      this.setState({ activeApplyBtn: true });
    }
  }

  applyFilter = () => {
    this.props.applyFilter(this.state);
  }


  render() {
    let filterItems = (
      <div>
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
        <div className="filter-content">

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
          <div className="composite-box">
            <label className="composite-label">Composite </label>
            <ToggleSwitch className="toggle-switch"
              isOn={this.state.isSelectCompositeOption}
              onToggle={this.switchChange.bind(this)} ></ToggleSwitch>
            <ButtonGroup className="action-button-group">
              <Button onClick={() => this.compositeOptionChange("OR")}
                active={this.state.selectedCompositeOption === "OR"}
                disabled={!this.state.isSelectCompositeOption}
              >OR</Button>
              <Button onClick={() => this.compositeOptionChange("AND")}
                active={this.state.selectedCompositeOption === "AND"}
                disabled={!this.state.isSelectCompositeOption}
              >AND</Button>
            </ButtonGroup>
          </div>
          <div className="filter-item-box">
            {filterItems}
            <div className="add-filter-box">
              <button className="feature-button-invert" onClick={this.addFilterItem}>+ Add</button>
            </div>
          </div>
        </div>
        <div className="limit-box">
          <label className="limit-label">Limit Within*:   </label>
          <input className="limit-input" type="number" min="0" value={this.state.minLimitValue}
            onChange={this.minLimitChanged}></input>
          <label className="value-seperator">-</label>
          <input className="limit-input" type="number" min="0" value={this.state.maxLimitValue}
            onChange={this.maxLimitChanged}></input>
          <div className = "spacer"></div>
          <button className="feature-button" onClick={this.applyFilter} disabled={!this.state.activeApplyBtn}>Apply</button>
        </div>
        {
          this.state.hasLimitError ?
            <div className="error-box">{this.state.limitErrorMsg}</div>
            : null
        }
      </div>
    );

  }
}

export default FilterContainer;
FilterContainer.propTypes = {
  filterColumns: PropTypes.array,
  applyFilter: PropTypes.func
};
