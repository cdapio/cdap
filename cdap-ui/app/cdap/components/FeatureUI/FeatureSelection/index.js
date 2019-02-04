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

/* eslint react/prop-types: 0 */
import React, { Component } from 'react';
import FilterContainer from './FilterContainer';
import './FeatureSelection.scss';
import GridHeader from './GridHeader';
import GridContainer from './GridContainer';
import { isNil, cloneDeep } from 'lodash';
import {
  GET_PIPE_LINE_FILTERED,
  GET_FEATURE_CORRELAION,
  GET_PIPE_LINE_DATA,
  GET_PIPELINE
} from '../config';
import { TabContent, TabPane, Nav, NavItem, NavLink } from 'reactstrap';
import classnames from 'classnames';
import CorrelationContainer from './CorrelationContainer';
import FEDataServiceApi from '../feDataService';
import NamespaceStore from 'services/NamespaceStore';
import { checkResponseError, getErrorMessage } from '../util';
import SaveFeatureModal from './SaveFeatureModal';

class FeatureSelection extends Component {

  filterColumnDefs = [];
  filterGridRows = [];
  correlationColumnDefs = [];
  correlationGridRows = [];

  constructor(props) {
    super(props);
    const dataInfo = this.dataParser(this.props.pipeLineData);
    this.state = Object.assign({
      activeTab: "1", selectedFeatures: [],
      openSaveModal: false,
      enableSave:false
    }, dataInfo);


    this.storeGridInfo(true, dataInfo.gridColumnDefs, dataInfo.gridRowData);
    this.storeGridInfo(false, dataInfo.gridColumnDefs, dataInfo.gridRowData);
  }

  dataParser = (data) => {
    const columDefs = [];
    const rowData = [];
    const columns = [];
    const featureNames = [];

    let rowCount = 0;
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
          columDefs.push({ headerName: element.name, field: element.name , resizable: true});
        });
      }

      // generate grid data
      if (!isNil(item.featureStatistics)) {
        // let counter = 0;
        const rowObj = { featureName: item.featureName };
        featureNames.push({ id: rowCount, name: item.featureName, selected: false });
        columns.forEach(element => {
          rowObj[element.name] = item.featureStatistics[element.name];
        });
        rowData.push(rowObj);
        rowCount++;
      }
    });

    return {
      gridColumnDefs: columDefs,
      gridRowData: rowData,
      filterColumns: columns,
      featureNames: featureNames
    };
  }

  storeGridInfo(isFilter, columDefs, rows) {
    if(isFilter){
      this.filterColumnDefs = cloneDeep(columDefs);
      this.filterGridRows = cloneDeep(rows);
    }
    else {
      this.correlationColumnDefs = cloneDeep(columDefs);
      this.correlationGridRows = cloneDeep(rows);
    }
  }

  navigateToParentWindow = () => {
    this.props.nagivateToParent();
  }

  gridRowSelectionChange = (selectedRows) => {
    if (!isNil(selectedRows) && selectedRows.length > 0) {
      this.setState({ selectedFeatures: selectedRows, enableSave:true });

    } else {
      this.setState({ selectedFeatures: [], enableSave: false });
    }
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
      compositeType: value.selectedCompositeOption,
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
            this.storeGridInfo(true, parsedResult.gridColumnDefs, parsedResult.gridRowData);
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
      if(tab == '1'){
        this.setState({ gridColumnDefs: this.filterColumnDefs, gridRowData: this.filterGridRows, activeTab: tab });
      }else {
        this.setState({ gridColumnDefs: this.correlationColumnDefs, gridRowData: this.correlationGridRows, activeTab: tab });
      }
    }
  }

  applyCorrelation = (value) => {
    const featureGenerationPipelineName = !isNil(this.props.selectedPipeline) ? this.props.selectedPipeline.pipelineName : "";
    const selectedFeatures = [value.selectedfeatures.name];
    FEDataServiceApi.featureCorrelationData(
      {
        namespace: NamespaceStore.getState().selectedNamespace,
        pipeline: featureGenerationPipelineName,
        coefficientType: value.coefficientType.name
      }, selectedFeatures).subscribe(
        result => {
          if (checkResponseError(result) || isNil(result["featureCorrelationScores"])) {
            this.handleError(result, GET_FEATURE_CORRELAION);
          } else {
            const parsedResult = this.praseCorrelation(result["featureCorrelationScores"]);
            this.storeGridInfo(false, parsedResult.gridColumnDefs, parsedResult.gridRowData);
            this.setState({ gridColumnDefs: parsedResult.gridColumnDefs, gridRowData: parsedResult.gridRowData });
          }
        },
        error => {
          this.handleError(error, GET_FEATURE_CORRELAION);
        }
      );
  }

  clearCorrelation = () => {
    this.onFeatureSelection(this.props.selectedPipeline);

  }


  handleError(error, type) {
    console.log(type, error);
    alert(getErrorMessage(error));
  }

  praseCorrelation = (value) => {
    const columDefs = [];
    const rowData = [];
    // generate column def\featureCorrelationScores
    if (!isNil(value) && value.length > 0) {
      const item = value[0]['featureCorrelationScores'];
      columDefs.push({ headerName: "Generated Feature", field: "featureName", width: 250, checkboxSelection: true });
      columDefs.push({ headerName: "Value", field: "value" });

      if (!isNil(item)) {
        for (let key in item) {
          rowData.push({ featureName: key, value: item[key] });
        }
      }
    }
    return {
      gridColumnDefs: columDefs,
      gridRowData: rowData
    };
  }

  onSaveClick =()=>{
    this.setState({openSaveModal:true});
  }

  onSaveModalClose = () => {
    this.setState({openSaveModal:false});
  }

  onFeatureSelection(pipeline, isFilter=false) {
    this.currentPipeline = pipeline;
    FEDataServiceApi.pipelineData({
      namespace: NamespaceStore.getState().selectedNamespace,
      pipeline: pipeline.pipelineName
    }).subscribe(
      result => {
        if (checkResponseError(result) || isNil(result["featureStatsList"])) {
          this.handleError(result, GET_PIPE_LINE_DATA);
        } else {
          const data = this.dataParser(result["featureStatsList"]);
          if(!isFilter){
            this.storeGridInfo(false, data.gridColumnDefs, data.gridRowData);
            this.setState({gridColumnDefs:data.gridColumnDefs, gridRowData: data.gridRowData });
          }
        }
      },
      error => {
        this.handleError(error, GET_PIPELINE);
      }
    );
  }


  render() {
    return (
      <div className="feature-selection-box">
        <div className="grid-box">
          <GridHeader selectedPipeline={this.props.selectedPipeline} backnavigation={this.navigateToParentWindow}
            save={this.onSaveClick} enableSave={this.state.enableSave}></GridHeader>
          <GridContainer gridColums={this.state.gridColumnDefs}
            rowData={this.state.gridRowData}
            selectionChange={this.gridRowSelectionChange}
          ></GridContainer>
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
              <CorrelationContainer applyCorrelation={this.applyCorrelation} featureNames={this.state.featureNames}
              onClear={this.clearCorrelation}></CorrelationContainer>
            </TabPane>
          </TabContent>
        </div>

        <SaveFeatureModal open={this.state.openSaveModal} message={this.state.alertMessage}
          onClose={this.onSaveModalClose} selectedPipeline={this.props.selectedPipeline}
          selectedFeatures={this.state.selectedFeatures}/>
      </div>
    );
  }
}

export default FeatureSelection;


