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
import DataPrepStore from 'components/DataPrep/store';
import shortid from 'shortid';
import ee from 'event-emitter';

require('./DataPrepTable.scss');

export default class DataPrepTable extends Component {
  constructor(props) {
    super(props);

    let storeState = DataPrepStore.getState().dataprep;

    this.state = {
      headers: storeState.headers,
      data: storeState.data,
      loading: !storeState.initialized,
      directivesLength: storeState.directives.length
    };

    this.eventEmitter = ee(ee);
    this.openWorkspace = this.openWorkspace.bind(this);

    this.sub = DataPrepStore.subscribe(() => {
      let state = DataPrepStore.getState().dataprep;
      this.setState({
        data: state.data,
        headers: state.headers,
        loading: !state.initialized,
        directivesLength: state.directives.length
      });
    });
  }

  componentWillUnmount() {
    this.sub();
  }

  openWorkspace() {
    this.eventEmitter.emit('DATAPREP_NO_WORKSPACE_ID');
  }

  render() {
    if (this.state.loading) {
      return (
        <div className="dataprep-table empty">
          <h4 className="text-xs-center">
            <span className="fa fa-spin fa-spinner" />
          </h4>
        </div>
      );
    }

    let headers = this.state.headers;
    let data = this.state.data;

    if (data.length === 0 || headers.length === 0) {
      return (
        <div className="dataprep-table empty">
          <h4 className="text-xs-center">No Data</h4>
          {
            this.state.directivesLength === 0 ? (
              <div>
                <h5 className="text-xs-center">Please create workspace and upload data</h5>
                <div className="button-container text-xs-center">
                  <button
                    className="btn btn-primary"
                    onClick={this.openWorkspace}
                  >
                    Create Workspace
                  </button>
                </div>
              </div>
            ) : null
          }
        </div>
      );
    }

    return (
      <div className="dataprep-table">
        <table className="table table-bordered table-striped">
          <thead className="thead-inverse">
            {
              headers.map((head) => {
                return (
                  <th
                    id={`column-${head}`}
                    key={head}
                  >
                    {head}
                  </th>
                );
              })
            }
          </thead>
          <tbody>
            {
              data.map((row) => {
                return (
                  <tr key={shortid.generate()}>
                    {headers.map((head) => {
                      return <td key={shortid.generate()}><div>{row[head]}</div></td>;
                    })}
                  </tr>
                );
              })
            }
          </tbody>
        </table>
      </div>
    );
  }
}
