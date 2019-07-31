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
import { AgGridReact } from 'ag-grid-react';
import isEmpty from 'lodash/isEmpty';
import findIndex from 'lodash/findIndex';
import 'ag-grid-community/dist/styles/ag-grid.css';
import 'ag-grid-community/dist/styles/ag-theme-balham.css';
import './GridContainer.scss';
import PropTypes from 'prop-types';
import CorrelationRenderer from 'components/FeatureUI/GridRenderers/CorrelationRenderer';

class GridContainer extends Component {
  gridApi;
  gridColumnApi;

  defaultColDef = {
    resizable: true
  }
  constructor(props) {
    super(props);
    this.state = {
      frameworkComponents: {
        'correlationRenderer': CorrelationRenderer,
      }
    };
  }

  componentWillReceiveProps(nextProps) {
    if (this.gridApi) {
      if (nextProps.isDataLoading) {
        this.gridApi.showLoadingOverlay();
      } else {
        if (isEmpty(nextProps.rowData)) {
          this.gridApi.showNoRowsOverlay();
        } else {
          this.gridApi.hideOverlay();
        }
      }
    }
  }

  onGridReady = params => {
    this.gridApi = params.api;
    this.gridColumnApi = params.columnApi;
    if (this.props.setFunctionPointer) {
      this.props.setFunctionPointer(this.setSelection.bind(this));
    }
  }

  setSelection(selectedValues, identifierCol) {
    const matchValue = {};
    if (this.gridApi) {
      this.gridApi.forEachNode((node, index) => {
        matchValue[identifierCol] = this.props.rowData[index][identifierCol];
        if (findIndex(selectedValues, matchValue ) < 0) {
          node.setSelected(false);
        } else {
          node.setSelected(true);
        }
      });
    }
  }

  onSelectionChanged = (data) => {
    this.props.selectionChange(data.api.getSelectedRows());
  }

  render() {
    return (
      <div
        className="ag-theme-balham grid-container"    >
        <AgGridReact
          suppressMenuHide={true}
          frameworkComponents={this.state.frameworkComponents}
          columnDefs={this.props.gridColums}
          defaultColDef={this.defaultColDef}
          enableFilter={true}
          rowSelection="multiple"
          rowData={this.props.rowData}
          onGridReady={this.onGridReady}
          onSelectionChanged={this.onSelectionChanged.bind(this)}>
        </AgGridReact>
      </div>
    );
  }
}

export default GridContainer;
GridContainer.propTypes = {
  isDataLoading: PropTypes.func,
  data: PropTypes.object,
  selectionChange: PropTypes.func,
  gridColums: PropTypes.array,
  rowData: PropTypes.array,
  identifierCol: PropTypes.string,
  setFunctionPointer: PropTypes.func
};
