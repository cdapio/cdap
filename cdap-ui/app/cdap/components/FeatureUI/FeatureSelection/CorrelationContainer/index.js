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
import { Dropdown, DropdownToggle, DropdownMenu, DropdownItem, ListGroup, ListGroupItem, InputGroup, Input } from 'reactstrap';
import './CorrelationContainer.scss';
import { isNil, cloneDeep } from 'lodash';
import PropTypes from 'prop-types';

class CorrelationContainer extends Component {
  algolist = [{ id: 1, name: "pearson" }, { id: 2, name: "spearman" }, { id: 3, name: "ChiSqTest" },{ id: 4, name: "mic" }, { id: 5, name: "anova" },{ id: 6, name: "kendallTau" }];
  correlationItems = [{ id: 1, enable: false, name: "TopN", minValue: "", maxValue: "", doubleView: false, hasRangeError: false },
  { id: 2, enable: false, name: "LowN", minValue: "", maxValue: "", doubleView: false, hasRangeError: false },
  { id: 3, enable: false, name: "Range", minValue: "", maxValue: "", doubleView: true, hasRangeError: false }
  ]

  lastSelectedFeature = undefined;

  constructor(props) {
    super(props);
    this.state = {
      algolist: this.algolist,
      openAlgoDropdown: false,
      selectedAlgo: { id: -1, name: 'Select' },
      selectedFeature: undefined,
      items: this.correlationItems,
      featureNames: cloneDeep(props.featureNames),
      activeApplyBtn: false
    };

  }


  toggleAlgoDropDown = () => {
    this.setState(prevState => ({
      openAlgoDropdown: !prevState.openAlgoDropdown
    }));
  }

  algoTypeChange = (item) => {
    this.setState({ selectedAlgo: item });
    setTimeout(() => {
      this.updateApplyBtnStatus();
    });
  }

  // toggleFeatureDropDown = () => {
  //   this.setState(prevState => ({
  //     openFeatureDropdown: !prevState.openFeatureDropdown
  //   }));
  // }

  // featureTypeChange = (item) => {
  //   this.setState({ selectedFeature: item });
  // }

  changeItem = (value, index) => {
    const itemList = [...this.state.items];
    const item = itemList[index];

    if (value.hasOwnProperty('enable')) {
      item['enable'] = value.enable;
    }

    if (value.hasOwnProperty('minValue')) {
      item['minValue'] = value.minValue.trim();
    }

    if (value.hasOwnProperty('maxValue')) {
      item['maxValue'] = value.maxValue.trim();
    }

    if (item.doubleView && item.minValue != '' && item.maxValue != '') {
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

    this.setState({ items: itemList });

  }

  onFeatrureClick(item) {
    if (this.lastSelectedFeature) {
      this.lastSelectedFeature.selected = false;
    }
    item.selected = true;
    this.lastSelectedFeature = item;
    this.setState({ selectedFeature: item });

    setTimeout(() => {
      this.updateApplyBtnStatus();
    });
  }

  onFeatureSearch = (evt) => {
    let value = "";
    if (!isNil(evt)) {
      value = evt.target.value.trim();
    }
    this.setState({ featureNames: this.props.featureNames.filter((item) => item.name.includes(value)) });
  }

  updateApplyBtnStatus = () => {
    let isValidFilterItems = true;

    if (this.state.selectedAlgo.id == -1) {
      isValidFilterItems = false;
    }

    if (isNil(this.state.selectedFeature)) {
      isValidFilterItems = false;
    }

    this.setState({ activeApplyBtn: isValidFilterItems });
  }


  applyCorrelation = () => {
    if (!isNil(this.props.applyCorrelation)) {
      const result = {
        coefficientType: this.state.selectedAlgo,
        selectedfeatures: this.state.selectedFeature
      };
      this.props.applyCorrelation(result);
    }
  }

  applyClear = () => {

    if (!isNil(this.lastSelectedFeature)) {
      this.lastSelectedFeature.enable = false;
    }

    this.setState({ featureNames: this.props.featureNames, selectedAlgo: { id: -1, name: 'Select' }, selectedfeature: undefined });

    setTimeout(() => {
      this.updateApplyBtnStatus();
    });

    this.props.onClear();

  }


  render() {
    // let corelationItem = (
    //   <div className="correlation-item-box">
    //     {
    //       this.state.items.map((item, index) => {
    //         return (<CorrelationItem
    //           itemVO={item}
    //           itemIndex={index}
    //           changeItem={this.changeItem.bind(this)}
    //           key={'fi_' + item.id}>
    //         </CorrelationItem>);
    //       })
    //     }
    //   </div>
    // );



    return (
      <div className="correlation-container">
        <div className="correlation-box">
          <div className="algo-box">
            <label className="algo-label">Algorithm: </label>
            <Dropdown isOpen={this.state.openAlgoDropdown} toggle={this.toggleAlgoDropDown}>
              <DropdownToggle caret>
                {this.state.selectedAlgo.name}
              </DropdownToggle>
              <DropdownMenu>
                {
                  this.state.algolist.map((column) => {
                    return (
                      <DropdownItem onClick={this.algoTypeChange.bind(this, column)}
                        key={'algo_' + column.id.toString()}
                      >{column.name}</DropdownItem>
                    );
                  })
                }
              </DropdownMenu>
            </Dropdown>
          </div>
          <div className="feature-box">
            <div>
              <label className="feature-label">Select Feature: </label>
              <InputGroup className="search-group">
                <Input className="search-input" placeholder="search generated feature" onChange={this.onFeatureSearch.bind(this)} />
                <i className="search-icon fa fa-search"></i>
              </InputGroup>
            </div>

            <ListGroup>
              {
                this.state.featureNames.map((item) => {
                  return (<ListGroupItem active={item.selected} key={item.id}
                    onClick={() => this.onFeatrureClick(item)}>
                    <label className='feature-box-item' title={item.name}>{item.name} </label>
                    {
                      item.selected && <i className="fa fa-check select-icon"></i>
                    }
                  </ListGroupItem>);
                })
              }
            </ListGroup>

          </div>
        </div>
        {
          // corelationItem
        }
        <div className="control-box">
          <button className="feature-button" onClick={this.applyCorrelation} disabled={!this.state.activeApplyBtn}>Apply</button>
          <button className="feature-button clear-button" onClick={this.applyClear} >Clear</button>
        </div>
      </div>
    );
  }
}

export default CorrelationContainer;

CorrelationContainer.propTypes = {
  applyCorrelation: PropTypes.func,
  featureNames: PropTypes.array,
  onClear: PropTypes.func
};

