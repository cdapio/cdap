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
import 'ag-grid-community/dist/styles/ag-grid.css';
import 'ag-grid-community/dist/styles/ag-theme-balham.css';
import './GridContainer.scss';
import PropTypes from 'prop-types';

class GridContainer extends Component {
  gridApi;
  gridColumnApi;
  defaultColDef = {
    resizable: true
  }
  constructor(props) {
    super(props);
  }

  componentWillReceiveProps(nextProps) {
    if (this.gridApi) {
      if (nextProps.isDataLoading) {
        this.gridApi.showLoadingOverlay();
      } else {
        if (isEmpty(nextProps.data)) {
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
  }

  onSelectionChanged = (data) => {
    this.props.selectionChange(data.api.getSelectedRows());
  }

  render() {
    return (
      <div
        className="ag-theme-balham grid-container"    >
        <AgGridReact
          suppressMenuHide = {true}
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
  rowData: PropTypes.array
};
