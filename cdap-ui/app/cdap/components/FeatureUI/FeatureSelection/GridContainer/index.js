/* eslint react/prop-types: 0 */
import React, { Component } from 'react';
import { AgGridReact } from 'ag-grid-react';
import 'ag-grid-community/dist/styles/ag-grid.css';
import 'ag-grid-community/dist/styles/ag-theme-balham.css';
import './GridContainer.scss';

class GridContainer extends Component {
  defaultColDef = {
    resizable: true
  }
  constructor(props) {
    super(props);
  }

  refreshGridColumns = (data) => {
    console.log(data);
  }

  onSelectionChanged = (data) => {
    this.props.selectionChange(data.api.getSelectedRows());
  }

  render() {
    return (
      <div
        className="ag-theme-balham grid-container"    >
        <AgGridReact
          columnDefs={this.props.gridColums}
          defaultColDef={this.defaultColDef}
          rowSelection="multiple"
          rowData={this.props.rowData}
          onSelectionChanged={this.onSelectionChanged.bind(this)}>
        </AgGridReact>
      </div>
    );
  }
}

export default GridContainer;
