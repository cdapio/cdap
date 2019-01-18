import React, { Component } from 'react';
import { Dropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import './CorrelationContainer.scss';
import { isNil } from 'lodash';
import propTypes from 'prop-types';
import CorrelationItem from '../CorrelationItem';


class CorrelationContainer extends Component {
  algolist = [{ id: 1, name: "pearson" }, { id: 2, name: "spearman" }];
  correlationItems = [{id:1, enable: false, name: "TopN", minValue: "", maxValue: "", doubleView: false, hasRangeError:false },
  {id:2, enable: false, name: "LowN", minValue: "", maxValue: "", doubleView: false, hasRangeError:false },
  {id:3, enable: false, name: "Range", minValue: "", maxValue: "", doubleView: true, hasRangeError:false }
  ]

  constructor(props) {
    super(props);
    this.state = {
      algolist: this.algolist,
      openAlgoDropdown: false,
      selectedAlgo: { id: -1, name: 'Select' },
      items:this.correlationItems
    };
  }


  toggleAlgoDropDown = () => {
    this.setState(prevState => ({
      openAlgoDropdown: !prevState.openAlgoDropdown
    }));
  }

  algoTypeChange = (item) => {
    this.setState({ selectedAlgo: item });
  }

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

    if (item.doubleView  && item.minValue != '' && item.maxValue != '') {
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

  applyCorrelation = () => {
    if (!isNil(this.props.applyCorrelation)) {
      this.props.applyCorrelation(this.state.selectedAlgo);
    }
  }


  render() {
    let corelationItem = (
      <div className="correlation-item-box">
        {
          this.state.items.map((item,index) => {
            return (<CorrelationItem
              itemVO={item}
              itemIndex={index}
              changeItem={this.changeItem.bind(this)}
              key={'fi_' + item.id}>
            </CorrelationItem>);
          })
        }
      </div>
    );



    return (
      <div className="correlation-container">
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
        {
            corelationItem
          }
        <div className="control-box">
          <button className="feature-button" onClick={this.applyCorrelation}>Apply</button>
        </div>
      </div>
    );
  }
}

export default CorrelationContainer;

CorrelationContainer.propTypes = {
  applyCorrelation: propTypes.func,
};

