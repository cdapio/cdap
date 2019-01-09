/*
 * Copyright © 2018 Cask Data, Inc.
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
