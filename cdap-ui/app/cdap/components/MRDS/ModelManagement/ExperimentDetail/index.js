import 'ag-grid-community/dist/styles/ag-grid.css';
import 'ag-grid-community/dist/styles/ag-theme-balham.css';
import PropTypes from 'prop-types';
import { AgGridReact } from 'ag-grid-react';
import { cloneDeep, find, findIndex, isNil } from 'lodash';
import React, { Component } from 'react';
import FeatureColumnModal from '../FeatureColumnModal/index';
import { hyperParameterFormater, metricParameterFormater, stringToObj } from '../utils/commonUtils';
import { DATASET_SCHEMA_LIST_COLUMN_DEF, MODEL_DETAIL_COLUMN_DEF } from './config';
import { ROW_HEIGHT, HEADER_HEIGHT } from 'components/MRDS/ModelManagementStore/constants';
require('./ExperimentDetail.scss');

class ExperimentDetail extends Component {
  rowid = 0;
  datasetGridApi;
  datasetGridColumnApi;
  modelGridApi;
  modelGridColumnApi;

  constructor(props) {
    super(props);
    const detail = this.getDetailWithDatasetSchemaList(this.props.detail);
    this.state = {
      experimentDetail: detail,
      context: { componentParent: this },
      openModelModal: false,
      modelDetails: {},
      detailPartitionLeft: [
        this.createData('Name', detail, 'experimentName'),
        this.createData('Description', detail, 'experimentDescription'),
        this.createData('Dataset', detail, 'datasetName'),
        this.createData('Prediction Column', detail, 'predictionField'),
      ],
      detailPartitionRight: [
        this.createData('Version', detail, 'latestModelId'),
        this.createData('Name', detail, 'latestModelName'),
        this.createData('Description', detail, 'latestModelDescription'),
        this.createData('Algorithm', detail, 'latesetModelAlgo'),

      ],
      datasetSchemaListColumn: DATASET_SCHEMA_LIST_COLUMN_DEF,
      datasetSchemaListRowData: detail.datasetSchemaList,
      modelDetailColumn: this.getModelDetailColumnDef(detail),
      modelDetailData: detail && detail.models ? detail.models : [],
      rowHeight: ROW_HEIGHT,
      headerHeight: HEADER_HEIGHT,
    };
  }

  createData(name, detail, property, greyedParam = undefined) {
    const id = this.rowid += 1;
    const value = detail ? detail[property] : '';
    return { id, name, value, greyedParam };
  }

  getColumnDef(columnDefs, column, headerName, fieldName) {
    const index = findIndex(columnDefs, ['headerName', headerName]);
    const keys = Object.keys(column);
    for (let i = 0; i < keys.length; i++) {
      let obj = {
        headerName: keys[i],
        field: fieldName + keys[i],
        valueFormatter: hyperParameterFormater,
        columnGroupShow: i == 0 ? 'closed,open' : 'open',
        resizable: true
      };
      columnDefs[index].children.push(obj);
    }
    return columnDefs;
  }

  getMetricsColumnDef(columnDefs, column, headerName, fieldName) {
    const index = findIndex(columnDefs, ['headerName', headerName]);
    const keys = Object.keys(column);
    for (let i = 0; i < keys.length; i++) {
      // for inner keys
      let innerObj = stringToObj(column[keys[i]]);
      const innerKeys = Object.keys(innerObj);
      innerKeys.forEach(element => {
        let obj = {
          headerName: `${keys[i]}_${element}`,
          field: fieldName + keys[i],
          valueFormatter: metricParameterFormater,
          columnGroupShow: i == 0 ? 'closed,open' : 'open',
          resizable: true
        };
        columnDefs[index].children.push(obj);
      });

    }
    return columnDefs;
  }

  getModelDetailColumnDef(detail) {
    let columnDefs = cloneDeep(MODEL_DETAIL_COLUMN_DEF);
    if (!isNil(detail) && !isNil(detail.models) && detail.models.length > 0) {
      const sampleModel = detail.models[0];
      columnDefs = this.getMetricsColumnDef(columnDefs, sampleModel.customMetrics, 'Metrics', 'customMetrics.');
      columnDefs = this.getColumnDef(columnDefs, sampleModel.hyperparameters, 'Hyper-parameter', 'hyperparameters.', '.default');
    }
    return columnDefs;
  }

  getDetailWithDatasetSchemaList(detail) {

    if (isNil(detail)) {
      return detail;
    }
    const rows = [];
    // check for prediction column
    if (!isNil(detail.predictionField)) {
      rows.push({ name: detail.predictionField, type: detail.predictionFieldType, featureORprediction: 'p' });
    }

    // check for feature column
    if (!isNil(detail.featureColumns)) {
      let featureColumns = detail.featureColumns.split(',');
      featureColumns.forEach(element => {
        let keyValues = element.split(':');
        if (keyValues.length > 0 && keyValues[0].trim() != "") {
          rows.push({ name: keyValues[0], type: keyValues.length > 1 ? keyValues[1] : 'NA', featureORprediction: 'f' });
        }
      });
    }

    // check for dataset schema

    if (!isNil(detail.datasetSchema)) {
      let schemaColumns = detail.datasetSchema.split(',');
      schemaColumns.forEach(element => {
        let keyValues = element.split(':');
        if (keyValues.length > 0 && keyValues[0].trim() != "") {
          if (!find(rows, { name: keyValues[0] })) {
            rows.push({ name: keyValues[0], type: keyValues.length > 1 ? keyValues[1] : 'NA', featureORprediction: '' });
          }
        }
      });
    }

    detail.datasetSchemaList = rows;
    return detail;
  }



  onDatasetGridReady = params => {
    this.datasetGridApi = params.api;
    this.datasetGridColumnApi = params.columnApi;
    this.datasetGridApi.sizeColumnsToFit();
  }

  onModelGridReady = params => {
    this.modelGridApi = params.api;
    this.modelGridColumnApi = params.columnApi;
    // this.modelGridApi.sizeColumnsToFit();
  }


  renderPartition(partitionList) {
    return (partitionList.map(row => (
      <div className="topBoxRow" key={row.name}>
        <label className="row-key">{row.name} </label>
        <div className={row.name === 'Version' ? 'row-value row-value-latest' : 'row-value'}>{row.value}</div>
      </div>
    )));
  }

  navigateToExpPage = () => {
    if (this.props.navigateToParent) {
      this.props.navigateToParent();
    }
  }

  gridCellClick(event) {
    if (event.colDef.field === 'modelId') {
      this.setState({
        openModelModal: true,
        modelDetails: event.data
      });
    }
  }

  render() {
    return (
      <div className='experiment-detail'>
        <div className='detail-partition'>
          <div className='left-partition'>
            <div className='left-header'>
              <label>Experiment</label>
            </div>
            <div className="topBox">
              {this.renderPartition(this.state.detailPartitionLeft)}
            </div>
          </div>
          <div className='partition-line'></div>
          <div className='right-partition'>
            <div className='right-header'>
              <label>Latest Model</label>
              <button className="back-btn" onClick={this.navigateToExpPage}>Back</button>
            </div>
            <div className="topBox">
              {this.renderPartition(this.state.detailPartitionRight)}
            </div>
          </div>
        </div>
        <div className='grid-partition'>
          <div className='dataset-detail'>
            <div className='grid-label'>Dataset Schema</div>
            <div className="ag-theme-material dataset-column-grid">
              <AgGridReact
                columnDefs={this.state.datasetSchemaListColumn}
                rowData={this.state.datasetSchemaListRowData}
                onGridReady={this.onDatasetGridReady}
                rowHeight={this.state.rowHeight}
                headerHeight={this.state.headerHeight}
              >
              </AgGridReact>
            </div>
            <div className="legend-box">
              <span className="legend-key">p</span> : <span className="legend-label">Prediction Column</span>
              <span className="legend-key">f</span> : <span className="legend-label">Feature Column</span>
            </div>
          </div>
          <div className='model-detail'>
            <div className='grid-label'>All Models</div>
            <div className="ag-theme-material model-detail-grid">
              <AgGridReact
                columnDefs={this.state.modelDetailColumn}
                rowData={this.state.modelDetailData}
                context={this.state.context}
                frameworkComponents={this.state.frameworkComponents}
                onGridReady={this.onModelGridReady}
                rowHeight={this.state.rowHeight}
                headerHeight={this.state.headerHeight}
                suppressSizeToFit='true'
                onCellClicked={this.gridCellClick.bind(this)}
              >
              </AgGridReact>
            </div>
          </div>

        </div>

        <FeatureColumnModal open={this.state.openModelModal} data={this.state.modelDetails} />
      </div>

    );
  }
}
export default ExperimentDetail;
ExperimentDetail.propTypes = {
  detail: PropTypes.any,
  navigateToParent: PropTypes.func,
};
