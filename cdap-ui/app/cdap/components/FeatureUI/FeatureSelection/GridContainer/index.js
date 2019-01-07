/* eslint react/prop-types: 0 */
import React, { Component } from 'react';
import { AgGridReact } from 'ag-grid-react';
import 'ag-grid-community/dist/styles/ag-grid.css';
import 'ag-grid-community/dist/styles/ag-theme-balham.css';
import './GridContainer.scss';

class GridContainer extends Component {
    constructor(props) {
        super(props);

        this.state = {
          columnDefs:this.props.gridColums,
          rowData:this.props.rowData
        };
    }

    refreshGridColumns = (data) => {
      console.log(data);
    }

    onSelectionChanged = (data) => {
      alert("I am an alert box!");
      console.log(data);
    }

    render() {
        return (
                <div
                  className="ag-theme-balham grid-container"    >
                    <AgGridReact
                        columnDefs={this.state.columnDefs}
                        rowSelection="multiple"
                        rowData={this.state.rowData}
                        onSelectionChanged={this.onSelectionChanged.bind(this)}>
                    </AgGridReact>
                </div>
            );
    }
}

export default GridContainer;
