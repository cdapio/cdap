/* eslint react/prop-types: 0 */
import React, { Component } from 'react';
import FilterContainer from './FilterContainer';
import './FeatureSelection.scss';
import GridHeader from './GridHeader';
import GridContainer from './GridContainer';
import { isNil } from 'lodash';
import {
  SERVER_IP,
  GET_PIPE_LINE_FILTERED,
  GET_PIPE_LINE_CORRELATED_DATA,
  GET_PIPELINE
} from '../config';
import { TabContent, TabPane, Nav, NavItem, NavLink } from 'reactstrap';
import classnames from 'classnames';
import CorrelationContainer from './CorrelationContainer';
import FEDataServiceApi from '../feDataService';
import NamespaceStore from 'services/NamespaceStore';
import { checkResponseError,getErrorMessage } from '../util';

class FeatureSelection extends Component {


  constructor(props) {
    super(props);
    this.state = Object.assign({ activeTab: "1" }, this.dataParser(this.props.pipeLineData));
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
          columDefs.push({ headerName: "Generated Feature", field: "featureName", width: 250, checkboxSelection: true });
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
      filterColumns: columns
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
    let result = {};
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

    result = {
      startPosition: value.minLimitValue.trim() == "" ? 0 : Number(value.minLimitValue.trim()),
      endPosition: value.maxLimitValue.trim() == "" ? 0 : Number(value.maxLimitValue.trim()),
      isComposite: true,
      compositeType: "OR",
      filterList: filtersList
    };
    if (value.selectedOrderbyColumn.id != -1) {
      result['orderByStat'] = value.selectedOrderbyColumn.name.replace(/\s/g, "");
    }

    return result;
  }

  getFilteredRecords(requestObj) {
    const featureGenerationPipelineName = !isNil(this.props.selectedPipeline) ? this.props.selectedPipeline.pipelineName : "";
    FEDataServiceApi.pipelineFilteredData(
      {
        namespace: NamespaceStore.getState().selectedNamespace,
        pipeline: featureGenerationPipelineName,
      }, requestObj).subscribe(
        result => {
          if (checkResponseError(result) || isNil(result["featureStatsList"])) {
            this.handleError(result, GET_PIPE_LINE_FILTERED);
          } else {
            const parsedResult = this.dataParser(result["featureStatsList"]);
            this.setState({ gridRowData: parsedResult.gridRowData });
          }
        },
        error => {
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

  applyCorrelation = (value) => {
    const featureGenerationPipelineName = !isNil(this.props.selectedPipeline) ? this.props.selectedPipeline.pipelineName : "";
    const URL = SERVER_IP + GET_PIPE_LINE_CORRELATED_DATA + featureGenerationPipelineName + '&correlationCoefficient=' + value.name;

    fetch(URL)
      .then(res => res.json())
      .then(
        (result) => {
          if (isNil(result) || isNil(result["featureStatsList"])) {
            alert("Pipeline Data Error");
          } else {
            this.setState({
              pipeLineData: result["featureStatsList"],
              data: result["featureStatsList"],
              selectedPipeline: this.currentPipeline,
              displayFeatureSelection: true
            });
          }
        },
        (error) => {
          this.handleError(error, GET_PIPELINE);
        }
      );
  }


  handleError(error, type) {
    console.log(type,error);
    alert(getErrorMessage(error));
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
              <CorrelationContainer applyCorrelation={this.applyCorrelation}></CorrelationContainer>
            </TabPane>
          </TabContent>
        </div>
      </div>
    );
  }
}

export default FeatureSelection;


