import React, { Component } from 'react';
import FilterContainer from './FilterContainer';
import './FeatureSelection.scss';
import GridHeader from './GridHeader';
import GridContainer from './GridContainer'
import { isNil } from 'lodash'

class FeatureSelection extends Component {
  //filterColumnList = [{id:1, name:'max'},{id:2, name:'min'},{id:3, name:'percentile'}, {id:4, name:'variance'}]

  constructor(props) {
    super(props)
    this.state = {
      dummy: "hello",
      gridColumnDefs: [],
      gridRowData: [],
      filterColumns: []//this.filterColumnList
    }
    //this.dataParser(this.sampleData);
  }

  dataParser = (data) => {
    const columDefs = [];
    const rowData = [];
    const columns = [];

    data.forEach(item => {
      if (columDefs.length <= 0) {
        // generate filter column
        if (!isNil(item.featureStatistics)) {
          let counter = 0;
          for (let key in item.featureStatistics) {
            columns.push({ id: counter, name: key });
            counter++;
          }
        }

        // generate column def
        if (!isNil(item.featureName)) {
          columDefs.push({ headerName: "featureName", field: "featureName", width: 250, checkboxSelection: true })
        }
        columns.forEach(element => {
          columDefs.push({ headerName: element.name, field: element.name })
        });
      }

      // generate grid data
      if (!isNil(item.featureStatistics)) {
        //let counter = 0;
        const rowObj = { featureName: item.featureName };
        columns.forEach(element => {
          rowObj[element.name] = item.featureStatistics[element.name]
        });

        rowData.push(rowObj);
      }


    });

    this.setState({
      gridColumnDefs: columDefs,
      gridRowData: rowData,
      filterColumns: columns
    })
  }

  navigateToParentWindow = ()=> {
    this.props.nagivateToParent();
  }

  componentDidMount() {
    this.dataParser(this.props.pipeLineData)
    console.log("call feature selection mount");
  }



  render() {
    return (
      <div className="feature-selection-box">
        <div className="grid-box">
          <button onClick={this.navigateToParentWindow}>Back</button>
          <GridHeader></GridHeader>
          <GridContainer></GridContainer>
        </div>
        <div className="filter-box">
           <FilterContainer ></FilterContainer>
        </div>
      </div>

    )
  }
}

export default FeatureSelection;
