/* eslint react/prop-types: 0 */
import React, { Component } from 'react';
import { Dropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import { isNil } from 'lodash';
import './FilterItem.scss';


class FilterItem extends Component {

    state = {
        filterTypeOpen: false,
        columnOpen: false,
        doubleView: false,
    };

    toggleFilterTypeDropDown = () => {
        this.setState(prevState => ({
            filterTypeOpen: !prevState.filterTypeOpen
        }));
    }

    toggleColumnDropDown = () => {
        this.setState(prevState => ({
            columnOpen: !prevState.columnOpen
        }));
    }

    filterTypeChange = (item) => {
        const mapItem = this.props.itemVO.filterViewMaps.find((element) => element.name === item.name);
        if (mapItem !== undefined && mapItem.view === 'double') {
            this.setState({ 'doubleView': true });
        } else {
            this.setState({ 'doubleView': false });
            //remove the max value
            this.maxValueChange(null);
        }
        const result = { selectedFilterType: item };
        this.props.changFilterItem(result, this.props.itemIndex);
    }

    filterColumnChange = (item) => {
        const result = { selectedFilterColumn: item };
        this.props.changFilterItem(result, this.props.itemIndex);
    }

    minValueChange = (evt) => {
        console.log("min value :: " + evt.target.value);
        const result = { minValue: evt.target.value };
        this.props.changFilterItem(result, this.props.itemIndex);
    }

    maxValueChange = (evt) => {
        let result = { maxValue: "" };
        if (!isNil(evt)) {
          result = { maxValue: evt.target.value };
        }
        this.props.changFilterItem(result, this.props.itemIndex);
    }



    render() {
        return (
            <div className="filterItm-box">
                <div className="filter-type-box">
                    <Dropdown className="filter-type-dropdown" isOpen={this.state.filterTypeOpen} toggle={this.toggleFilterTypeDropDown.bind(this)}>
                        <DropdownToggle caret>
                            {this.props.itemVO.selectedFilterType.name}
                        </DropdownToggle>
                        <DropdownMenu >
                            {
                                this.props.itemVO.filterTypeList.map((type) => {
                                    return (
                                        <DropdownItem onClick={this.filterTypeChange.bind(this, type)}
                                        key={'ft_'+type.id.toString()}
                                        >{type.name}</DropdownItem>
                                    );
                                })
                            }
                        </DropdownMenu>
                    </Dropdown>
                    {
                        this.props.itemIndex !== 0 ?
                            <img onClick={this.props.removeFilterItem} src="/cdap_assets/img/remove.svg" />
                            : null
                    }
                </div>
                <div className="filter-column-box">
                    <Dropdown className="filter-type-dropdown" isOpen={this.state.columnOpen} toggle={this.toggleColumnDropDown.bind(this)}>
                        <DropdownToggle caret>
                            {this.props.itemVO.selectedFilterColumn.name}
                        </DropdownToggle>
                        <DropdownMenu>
                            {
                                this.props.itemVO.filterColumnList.map((column) => {
                                    return (
                                        <DropdownItem onClick={this.filterColumnChange.bind(this, column)}
                                        key={'c_'+column.id.toString()}
                                        >{column.name}</DropdownItem>
                                    );
                                })
                            }
                        </DropdownMenu>
                    </Dropdown>
                    <label className="value-seperator">:</label>
                    <input className="value-input" type="number" min="0" value={this.props.itemVO.minValue}
                        onChange={this.minValueChange}></input>
                    {this.state.doubleView ?
                        <div>
                            <label className="value-seperator">-</label>
                            <input className="value-input"  type="number" min="0" value={this.props.itemVO.maxValue}
                                onChange={this.maxValueChange}></input>
                        </div>
                        : null
                    }
                </div>
            </div>
        );
    }
}

export default FilterItem;
