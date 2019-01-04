import React, { Component } from 'react';
import { Dropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import FilterItem from '../FilterItem/index'
import './FilterContainer.scss'
import {cloneDeep} from "lodash"

class FilterContainer extends Component {
    filterTypeList = [{id:1, name:'TopN'},{id:2, name:'LowN'},{id:3, name:'Range'}]
    filterColumnList =  [{id:1, name:'max'},{id:2, name:'min'},{id:3, name:'percentile'}, {id:4, name:'variance'}]
    filterViewMaps = [{name:'TopN', view:'single'}, {name:'LowN', view:'single'},{name:'Range', view:'double'}]

    constructor(props) {
        super(props);
        this.state = {
            orderbyOpen:false,
            selectedOrderbyColumn:{id:-1, name:'Select'},
            orderByCOlumnList:cloneDeep(this.filterColumnList),
            filterItemList: [this.getFilterItemVO()],
        }
      }


      toggleOrderbyDropDown = ()=>{
        this.setState(prevState => ({
          orderbyOpen: !prevState.orderbyOpen
      }));
      }

      orderbyColumnChange = (item) => {
        this.setState({selectedOrderbyColumn:item})
      }


    addFilterItem = () => {
        const filterItems = [...this.state.filterItemList];
        filterItems.push(this.getFilterItemVO())
        this.setState({filterItemList:filterItems});
    }

    getFilterItemVO = ()=>{
        return {filterTypeList:this.filterTypeList,
            selectedFilterType:{id:-1, name:'Select'},
            filterColumnList:this.filterColumnList,
            selectedFilterColumn:{id:-1, name:'Select'},
            filterViewMaps: this.filterViewMaps,
            minValue:'', maxValue:''
        }
    }

    changFilterItem = (value, index) =>{
        const itemList = [...this.state.filterItemList];
        const item = itemList[index];
        if(value.hasOwnProperty('selectedFilterType')){
            item['selectedFilterType'] = value.selectedFilterType
        }

        if(value.hasOwnProperty('selectedFilterColumn')){
            item['selectedFilterColumn'] = value.selectedFilterColumn
        }

        if(value.hasOwnProperty('minValue')){
            item['minValue'] = value.minValue;
        }

        if(value.hasOwnProperty('maxValue')){
            item['maxValue'] = value.maxValue;
        }


        this.setState({filterItemList:itemList});
    }

    removeFilterItem = (index)=> {
        const itemList = [...this.state.filterItemList];
        if(itemList.length>1){
            itemList.splice(index,1);
        }
        this.setState({filterItemList:itemList});
    }

    updateIntialState = () =>{
      this.setState({
        orderByCOlumnList: cloneDeep(this.filterColumnList),
        filterItemList:this.getFilterItemVO(),
      });
    }
    // componentDidMount() {
    //   this.filterColumnList = this.props.filterColumns;
    //   this.updateIntialState();
    //   console.log("call filter container mount");
    // }

    // componentWillReceiveProps(nextProps) {
    //   // Any time props.email changes, update state.
    //   console.log("state change challed from parent :: "+filterColumns)
    //   if (nextProps.filterColumns !== this.props.filterColumns) {
    //     this.filterColumnList = this.props.filterColumns;
    //     this.updateIntialState();
    //   }
    // }

    // componentWillMount(){
    //   this.parse

    // }


    render() {
     // let { filterColumns } = this.props;
      //this.filterColumnList = filterColumns;
      //this.updateIntialState();

        let filterItems = (
            <div className="filter-item-Container">
                {
                    this.state.filterItemList.map((item, index) => {
                        return <FilterItem
                                itemVO={item}
                                itemIndex={index}
                                changFilterItem={this.changFilterItem.bind(this)}
                                removeFilterItem={this.removeFilterItem.bind(this, index)}
                                key={'fi_'+index.toString()}>
                                </FilterItem>
                    })
                }
            </div>
        )

        return (
            <div className="filter-container">
                <h3>Filters</h3>
                <div className="orderby-box">
                    <label className="orderby-label">Orderby:   </label>
                     <Dropdown isOpen={this.state.orderbyOpen} toggle={this.toggleOrderbyDropDown}>
                        <DropdownToggle caret>
                            {this.state.selectedOrderbyColumn.name}
                        </DropdownToggle>
                        <DropdownMenu>
                            {
                                this.state.orderByCOlumnList.map((column) => {
                                    return (
                                        <DropdownItem onClick={this.orderbyColumnChange.bind(this, column)}
                                        key={'orderby_'+column.id.toString()}
                                        >{column.name}</DropdownItem>
                                    )
                                })
                            }
                        </DropdownMenu>
                    </Dropdown>

                </div>
                {filterItems}
                <button onClick={this.addFilterItem}>+Add</button>
            </div>
        )

    }

}

export default FilterContainer;
