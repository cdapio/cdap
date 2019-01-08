/* eslint react/prop-types: 0 */
import React, { Component } from 'react';
import FilterContainer from './FilterContainer';
import './FeatureSelection.scss';
import GridHeader from './GridHeader';
import GridContainer from './GridContainer';
import { isNil } from 'lodash';
import { SERVER_IP, GET_PIPE_LINE_FILTERED_DATA, GET_PIPE_LINE_FILTERED } from '../config';
import { TabContent, TabPane, Nav, NavItem, NavLink } from 'reactstrap';
import classnames from 'classnames';
import CorrelationContainer from './CorrelationContainer';

class FeatureSelection extends Component {
  constructor(props) {
    super(props);
    this.state = this.dataParser(this.props.pipeLineData);
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
          columDefs.push({ headerName: "featureName", field: "featureName", width: 250, checkboxSelection: true });
        }
        columns.forEach(element => {
          columDefs.push({ headerName: element.name, field: element.name });
        });
      }

      // generate grid data
      if (!isNil(item.featureStatistics)) {
        // let counter = 0;
        const rowObj = { featureName: item.featureName };
        columns.forEach(element => {
          rowObj[element.name] = item.featureStatistics[element.name];
        });
        rowData.push(rowObj);
      }
    });
    return {
      gridColumnDefs: columDefs,
      gridRowData: rowData,
      filterColumns: columns,
      activeTab: "1"
    };
  }

  navigateToParentWindow = () => {
    this.props.nagivateToParent();
  }

  applyFilter = (filterObj) => {
    const requestObject = this.requestGenerator(filterObj);
    this.getFilteredRecords(requestObject);
  }

  requestGenerator = (value) => {
    const filtersList = [];
    if (!isNil(value) && !isNil(value.filterItemList)) {
      value.filterItemList.forEach(element => {
        let low = 0;
        let upper = 0;
        if (element.selectedFilterType.name === 'Range') {
          low = element.minValue.trim() == "" ? 0 : Number(element.minValue.trim());
          upper = element.maxValue.trim() == "" ? 0 : Number(element.maxValue.trim());
        } else {
          upper = element.minValue.trim() == "" ? 0 : Number(element.minValue.trim());
        }

        filtersList.push({
          filterType: element.selectedFilterType.name,
          statsName: element.selectedFilterColumn.name.replace(/\s/g, ""),
          lowerLimit: low,
          upperLimit: upper,
        });
      });
    }
    return {
      orderByStat: value.selectedOrderbyColumn.name.replace(/\s/g, ""),
      startPosition: value.minLimitValue.trim() == "" ? 0 : Number(value.minLimitValue.trim()),
      endPosition: value.maxLimitValue.trim() == "" ? 0 : Number(value.maxLimitValue.trim()),
      isComposite: true,
      compositeType: "OR",
      filterList: filtersList
    };
  }

  getFilteredRecords(requestObj) {

    const featureGenerationPipelineName = !isNil(this.props.selectedPipeline) ? this.props.selectedPipeline.pipelineName : "";

    let URL = SERVER_IP + GET_PIPE_LINE_FILTERED_DATA + featureGenerationPipelineName + '/features/filter';

    fetch(URL, {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(requestObj)
    }).then(res => res.json())
      .then(
        (result) => {
          if (isNil(result) || isNil(result["featureStatsList"])) {
            alert("Pipeline filter Data Error");
          } else {
            // to do
          }
        },
        (error) => {
          this.handleError(error, GET_PIPE_LINE_FILTERED);
        }
      );
  }

  toggle(tab) {
    if (this.state.activeTab !== tab) {
      this.setState({
        activeTab: tab
      });
    }
  }

  render() {
    return (
      <div className="feature-selection-box">
        <div className="grid-box">
          <GridHeader selectedPipeline={this.props.selectedPipeline} backnavigation={this.navigateToParentWindow}></GridHeader>
          <GridContainer gridColums={this.state.gridColumnDefs} rowData={this.state.gridRowData}></GridContainer>
        </div>
        <div className="filter-box">
          <Nav tabs className="tab-header">
            <NavItem>
              <NavLink
                className={classnames({ active: this.state.activeTab === '1' })}
                onClick={() => { this.toggle('1'); }}>
                Filter
              </NavLink>
            </NavItem>
            <NavItem>
              <NavLink
                className={classnames({ active: this.state.activeTab === '2' })}
                onClick={() => { this.toggle('2'); }}>
                Correlation
              </NavLink>
            </NavItem>
          </Nav>
          <TabContent activeTab={this.state.activeTab} className="tab-content">
            <TabPane tabId="1" className="tab-pane">
              <FilterContainer filterColumns={this.state.filterColumns}
                applyFilter={this.applyFilter}></FilterContainer>
            </TabPane>
            <TabPane tabId="2" className="tab-pane">
              <CorrelationContainer></CorrelationContainer>
            </TabPane>
          </TabContent>
        </div>
      </div>
    );
  }
}

export default FeatureSelection;


