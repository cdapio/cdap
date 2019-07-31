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
import FilterContainer from './FilterContainer';
import './FeatureSelection.scss';
import GridHeader from './GridHeader';
import GridContainer from './GridContainer';
import { isNil } from 'lodash';
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
import { checkResponseError, getDefaultRequestHeader } from '../util';
import SaveFeatureModal from './SaveFeatureModal';
import PropTypes from 'prop-types';
import { getRoundOfValue } from '../GridFormatters';
import ModelContainer from './ModelContainer';

class FeatureSelection extends Component {

  gridInfoList = [];
  targetVariable = "";
  totalStatsFeature = 0;
  identiferCol = "featureName";
  setGridSelection;

  constructor(props) {
    super(props);
    this.initGridInfo(3);   // Number of tabs
    const dataInfo = this.dataParser(this.props.pipeLineData);
    this.state = Object.assign({
      activeTab: "0", selectedFeatures: [],
      openSaveModal: false,
      enableSave: false,
      isDataLoading: false,
    }, dataInfo);

    this.updateGridInfo(0, dataInfo.gridColumnDefs, dataInfo.gridRowData);
    this.totalStatsFeature = dataInfo.gridRowData.length;
  }

  componentWillMount() {
    if (this.props.pipelineRequestConfig &&  this.props.pipelineRequestConfig.targetColumn && this.props.pipelineRequestConfig.targetColumn.column) {
      this.targetVariable = this.props.pipelineRequestConfig.targetColumn.column;
    }
  }

  initGridInfo(length) {
    for (let i=0; i < length; i++) {
      this.gridInfoList.push({
        columDefs: {},
        rowData: []
      });
    }
  }

  updateGridInfo(index, columDefs, rowData) {
    if (index < this.gridInfoList.length) {
      this.gridInfoList[index]['gridColumnDefs'] = columDefs;
      this.gridInfoList[index]['gridRowData'] = rowData;
    }
  }

  getGridInfo(index) {
    if (index < this.gridInfoList.length) {
      return this.gridInfoList[index];
    } else {
      return {
        gridColumnDefs: {},
        gridRowData: []
      };
    }
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
          columDefs.push({ headerName: "Generated Feature", field: this.identiferCol, width: 500, checkboxSelection: true, headerCheckboxSelection: true, headerCheckboxSelectionFilteredOnly: true, tooltipField: this.identiferCol });
        }
        columns.forEach(element => {
          columDefs.push({ headerName: element.name, field: element.name, resizable: true, filter: 'agNumberColumnFilter' });
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

  navigateToParentWindow = () => {
    this.props.nagivateToParent();
  }

  gridRowSelectionChange = (selectedRows) => {
    if (!isNil(selectedRows) && selectedRows.length > 0) {
      this.setState({ selectedFeatures: selectedRows, enableSave: true });

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
    this.setState({
      isDataLoading: true
    });
    FEDataServiceApi.pipelineFilteredData(
      {
        namespace: NamespaceStore.getState().selectedNamespace,
        pipeline: featureGenerationPipelineName,
      }, requestObj, getDefaultRequestHeader()).subscribe(
        result => {
          if (checkResponseError(result) || isNil(result["featureStatsList"])) {
            this.handleError(result, GET_PIPE_LINE_FILTERED);
            this.setState({
              isDataLoading: false,
              gridRowData: []
            });
          } else {
            const parsedResult = this.dataParser(result["featureStatsList"]);
            this.updateGridInfo(0, parsedResult.gridColumnDefs, parsedResult.gridRowData);
            this.setState({
              isDataLoading: false,
              gridRowData: parsedResult.gridRowData
            });
          }
        },
        error => {
          this.handleError(error, GET_PIPE_LINE_FILTERED);
          this.setState({
            isDataLoading: false,
            gridRowData: []
          });
        }
      );
  }

  toggle(tab) {
    if (this.state.activeTab !== tab) {
      const gridInfo = this.getGridInfo(Number(tab));
      this.setState({
        gridColumnDefs: gridInfo['gridColumnDefs'],
        gridRowData: gridInfo['gridRowData'],
        activeTab: tab
      });
      setTimeout(() => {
        this.setGridSelection(this.state.selectedFeatures, this.identiferCol);
        }, 500);
    }
  }

  applyCorrelation = (value) => {
    const featureGenerationPipelineName = !isNil(this.props.selectedPipeline) ? this.props.selectedPipeline.pipelineName : "";
    const selectedFeatures = [value.selectedfeatures];
    this.setState({
      isDataLoading: true
    });
    FEDataServiceApi.featureCorrelationData(
      {
        namespace: NamespaceStore.getState().selectedNamespace,
        pipeline: featureGenerationPipelineName,
        coefficientType: value.coefficientType.name
      }, selectedFeatures, getDefaultRequestHeader()).subscribe(
        result => {
          if (checkResponseError(result) || isNil(result["featureCorrelationScores"])) {
            this.handleError(result, GET_FEATURE_CORRELAION);
            this.setState({
              isDataLoading: false,
              gridRowData: [],
            });
          } else {
            const parsedResult = this.parseRelationalData(result["featureCorrelationScores"], 'featureCorrelationScores');
            this.updateGridInfo(1, parsedResult.gridColumnDefs, parsedResult.gridRowData);
            this.setState({
              isDataLoading: false,
              gridColumnDefs: parsedResult.gridColumnDefs,
              gridRowData: parsedResult.gridRowData
            });
            setTimeout(() => {
              this.setGridSelection(this.state.selectedFeatures, this.identiferCol);
              }, 500);
          }
        },
        error => {
          this.setState({
            isDataLoading: false,
            gridRowData: [],
          });
          this.handleError(error, GET_FEATURE_CORRELAION);
        }
      );
  }

  applyModelSelection = (value) => {
    const featureGenerationPipelineName = !isNil(this.props.selectedPipeline) ? this.props.selectedPipeline.pipelineName : "";
    const selectedFeatures = [value.selectedfeatures];
    this.setState({
      isDataLoading: true
    });
    FEDataServiceApi.modelBasedData(
      {
        namespace: NamespaceStore.getState().selectedNamespace,
        pipeline: featureGenerationPipelineName,
        coefficientType: value.coefficientType.name
      }, selectedFeatures, getDefaultRequestHeader()).subscribe(
        result => {
          if (checkResponseError(result) || isNil(result["featureImportanceScores"])) {
            this.handleError(result, GET_FEATURE_CORRELAION);
            this.setState({
              isDataLoading: false,
              gridRowData: [],
            });
          } else {
            const parsedResult = this.parseRelationalData(result["featureImportanceScores"], 'featureImportanceScores');
            this.updateGridInfo(2, parsedResult.gridColumnDefs, parsedResult.gridRowData);
            this.setState({
              isDataLoading: false,
              gridColumnDefs: parsedResult.gridColumnDefs,
              gridRowData: parsedResult.gridRowData
            });
            setTimeout(() => {
              this.setGridSelection(this.state.selectedFeatures, this.identiferCol);
              }, 500);
          }
        },
        error => {
          this.setState({
            isDataLoading: false,
            gridRowData: [],
          });
          this.handleError(error, GET_FEATURE_CORRELAION);
        }
      );
  }



  handleError(error, type) {
    console.log('error ==> ' + error + "| type => " + type);
  }

  parseRelationalData = (value, property) => {
    const columDefs = [];
    const rowData = [];
    let minValue;
    let maxValue;
    if (!isNil(value) && value.length > 0) {
      const respData = value[0][property];
      if (!isNil(respData)) {
        for (let key in respData) {
          if (isNil(minValue) || minValue > respData[key]) {
            minValue = respData[key];
          } if (isNil(maxValue) || maxValue < respData[key]) {
            maxValue = respData[key];
          }
          rowData.push({ featureName: key, value: respData[key] });
        }
      }
      if (minValue >= 0) {
        minValue = 0;
      } else {
        if ((minValue * -1) > maxValue) {
          maxValue = (minValue * -1);
        } else {
          minValue = (maxValue * -1);
        }
      }
      columDefs.push(
        {
          headerName: "Generated Feature",
          field: this.identiferCol,
          width: 450,
          checkboxSelection: true,
          headerCheckboxSelection: true,
          headerCheckboxSelectionFilteredOnly:
            true,
          tooltipField: this.identiferCol
        });
      columDefs.push(
        {
          field: "VO",
          headerName: "",
          cellRenderer: 'correlationRenderer',
          width: 440,
        });
      columDefs.push(
        {
          headerName: "Value",
          field: "value",
          width: 100,
          filter: 'agNumberColumnFilter',
          tooltipField: 'value',
          valueFormatter: function (params) { return getRoundOfValue(params); },
        });
    }
    return {
      gridColumnDefs: columDefs,
      gridRowData: rowData.map((item => {
        item['VO'] = { value: item['value'], min: minValue, max: maxValue };
        return item;
      }))
    };
  }

  onSaveClick = () => {
    this.setState({ openSaveModal: true });
  }

  onSaveModalClose = () => {
    this.setState({ openSaveModal: false });
  }

  onFeatureSelection(pipeline, isFilter = false) {
    this.currentPipeline = pipeline;
    this.setState({
      isDataLoading: true
    });
    FEDataServiceApi.pipelineData({
      namespace: NamespaceStore.getState().selectedNamespace,
      pipeline: pipeline.pipelineName
    }, {}, getDefaultRequestHeader()).subscribe(
      result => {
        if (checkResponseError(result) || isNil(result["featureStatsList"])) {
          this.handleError(result, GET_PIPE_LINE_DATA);
          this.setState({
            isDataLoading: false,
            gridRowData: []
          });
        } else {
          const data = this.dataParser(result["featureStatsList"]);
          if (!isFilter) {
            this.storeGridInfo(false, data.gridColumnDefs, data.gridRowData);
            this.setState({
              gridColumnDefs: data.gridColumnDefs,
              gridRowData: data.gridRowData,
              isDataLoading: false
            });
          }
        }
      },
      error => {
        this.handleError(error, GET_PIPELINE);
        this.setState({
          isDataLoading: false,
          gridRowData: []
        });
      }
    );
  }

  setGridFunction(childFunc) {
    this.setGridSelection  = childFunc;
  }

  render() {
     return (
      <div className="feature-selection-box">
        <div className="grid-box">
          <GridHeader selectedPipeline={this.props.selectedPipeline} backnavigation={this.navigateToParentWindow}
            selectedCount = { isNil(this.state.selectedFeatures)?this.totalStatsFeature:this.state.selectedFeatures.length }
            totalCount = { isNil(this.state.gridRowData) ? this.totalStatsFeature:this.state.gridRowData.length }
            save={this.onSaveClick} enableSave={this.state.enableSave}></GridHeader>
          <GridContainer gridColums={this.state.gridColumnDefs}
            rowData={this.state.gridRowData} setFunctionPointer = { this.setGridFunction.bind(this) }
            selectionChange={this.gridRowSelectionChange}
          ></GridContainer>
        </div>
        <div className="filter-box">
          <Nav tabs className="tab-header">
            <NavItem>
              <NavLink
                className={classnames({ active: this.state.activeTab === '0' })}
                onClick={() => { this.toggle('0'); }}>
                Statistics
              </NavLink>
            </NavItem>
            <NavItem>
              <NavLink
                className={classnames({ active: this.state.activeTab === '1' })}
                onClick={() => { this.toggle('1'); }}>
                Correlation
              </NavLink>
            </NavItem>
            <NavItem>
              <NavLink
                className={classnames({ active: this.state.activeTab === '2' })}
                onClick={() => { this.toggle('2'); }}>
                Model
              </NavLink>
            </NavItem>
          </Nav>
          <TabContent activeTab={this.state.activeTab} className="tab-content">
            <TabPane tabId="0" className="tab-pane">
              <FilterContainer filterColumns={this.state.filterColumns}
                applyFilter={this.applyFilter}></FilterContainer>
            </TabPane>
            <TabPane tabId="1" className="tab-pane">
              <CorrelationContainer applyCorrelation={this.applyCorrelation}
                targetVariable={this.targetVariable}
                featureNames={this.state.featureNames}
                ></CorrelationContainer>
            </TabPane>
            <TabPane tabId="2" className="tab-pane">
              <ModelContainer applyModelSelection={this.applyModelSelection}
                targetVariable={this.targetVariable}
                featureNames={this.state.featureNames}
                ></ModelContainer>
            </TabPane>
          </TabContent>
        </div>

        <SaveFeatureModal open={this.state.openSaveModal} message={this.state.alertMessage}
          onClose={this.onSaveModalClose} selectedPipeline={this.props.selectedPipeline}
          selectedFeatures={this.state.selectedFeatures} />
      </div>
    );
  }
}

export default FeatureSelection;
FeatureSelection.propTypes = {
  pipeLineData: PropTypes.array,
  nagivateToParent: PropTypes.func,
  selectedPipeline: PropTypes.object,
  pipelineRequestConfig: PropTypes.object
};
