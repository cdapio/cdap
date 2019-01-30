import React, { Component } from 'react';
import { Dropdown, DropdownToggle, DropdownMenu, DropdownItem,ListGroup, ListGroupItem, InputGroup, Input} from 'reactstrap';
import './CorrelationContainer.scss';
import { isNil, cloneDeep } from 'lodash';
import propTypes from 'prop-types';
import CorrelationItem from '../CorrelationItem';


class CorrelationContainer extends Component {
  algolist = [{ id: 1, name: "pearson" }, { id: 2, name: "spearman" }];
  correlationItems = [{ id: 1, enable: false, name: "TopN", minValue: "", maxValue: "", doubleView: false, hasRangeError: false },
  { id: 2, enable: false, name: "LowN", minValue: "", maxValue: "", doubleView: false, hasRangeError: false },
  { id: 3, enable: false, name: "Range", minValue: "", maxValue: "", doubleView: true, hasRangeError: false }
  ]

  lastSelectedFeature =  undefined;

  constructor(props) {
    super(props);
    this.state = {
      algolist: this.algolist,
      openAlgoDropdown: false,
      selectedAlgo: { id: -1, name: 'Select' },
      openFeatureDropdown: false,
      selectedFeature: undefined,
      items: this.correlationItems,
      featureNames: cloneDeep(props.featureNames)
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

  toggleFeatureDropDown = () => {
    this.setState(prevState => ({
      openFeatureDropdown: !prevState.openFeatureDropdown
    }));
  }

  featureTypeChange = (item) => {
    this.setState({ selectedFeature: item });
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
  }

  onFeatureSearch = (evt) => {
    let value = "";
    if (!isNil(evt)) {
      value = evt.target.value.trim();
    }
    this.setState({featureNames: this.props.featureNames.filter((item)=> item.name.includes(value))});
  }

  applyCorrelation = () => {
    if (!isNil(this.props.applyCorrelation)) {
      const result = {
        coefficientType: this.state.selectedAlgo,
        selectedfeatures: this.state.selectedFeature
      }
      this.props.applyCorrelation(result);
    }
  }


  render() {
    let corelationItem = (
      <div className="correlation-item-box">
        {
          this.state.items.map((item, index) => {
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
        <div className="feature-box">
        <div>
          <label className="feature-label">Select Feature: </label>
          <InputGroup className="search-group">
              <Input className="search-input" placeholder="search generated feature" onChange={this.onFeatureSearch.bind(this)} />
              <i className = "search-icon fa fa-search"></i>
            </InputGroup>
        </div>

          <ListGroup>
            {
              this.state.featureNames.map((item) => {
                return (<ListGroupItem active={item.selected} key={item.id}
                  onClick={() => this.onFeatrureClick(item)}>
                  <label className='feature-box-item'>{item.name} title={item.name}</label>
                    {
                      item.selected && <i class="fa fa-check select-icon"></i>
                    }
                </ListGroupItem>);
              })
            }
          </ListGroup>

        </div>
        {
          //corelationItem
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

