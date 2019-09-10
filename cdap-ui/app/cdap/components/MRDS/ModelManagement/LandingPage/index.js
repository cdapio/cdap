/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
import PropTypes from 'prop-types';
import 'ag-grid-community/dist/styles/ag-theme-material.css';
import { EXP_COLUMN_DEF } from './config';
import { isNil } from 'lodash';
import { HEADER_HEIGHT, ROW_HEIGHT } from 'components/MRDS/ModelManagementStore/constants';
import ExperimentDetail from '../ExperimentDetail';
import MRDSServiceApi from 'components/MRDS/mrdsServiceApi';
import NamespaceStore from 'services/NamespaceStore';
import { getDefaultRequestHeader } from '../../config';

require('./Landing.scss');

class LandingPage extends Component {
  gridApi;
  gridColumnApi;
  experimentDetail;
  baseUrl = "";

  constructor(props) {
    super(props);
    this.state = {
      columnDefs: EXP_COLUMN_DEF,
      experiments: [],
      numberOfExperiment: 0,
      context: { componentParent: this },
      isRouteToExperimentDetail: false,
      isFeatureColumnModal: false,
      featuredColumns: '',
      rowHeight: ROW_HEIGHT,
      headerHeight: HEADER_HEIGHT
    };
  }


  componentWillMount() {
    this.getExperimentDetails();
  }

  handleError(error, type) {
    console.log('error ==> ' + error + "| type => " + type);
  }

  getExperimentDetails() {
    MRDSServiceApi.fetchExperimentsDetails({
      namespace: NamespaceStore.getState().selectedNamespace,
    }, {}, getDefaultRequestHeader()).subscribe(
      result => {
        if (!isNil(result) && result.length > 0) {
          this.setState({
            experiments: result
          });
        } else {
          this.setState({
            experiments: []
          });
        }
      },
      error => {
        this.handleError(error, "Getting Experiment Details");
      }
    );
  }

  onGridReady = params => {
    this.gridApi = params.api;
    this.gridColumnApi = params.columnApi;
    this.gridApi.sizeColumnsToFit();
  }

  gridCellClick(event) {
    if (event.colDef.field === 'experimentName') {
      this.setState({
        isRouteToExperimentDetail: true,
        experimentDetail: event.data
      });
    }
  }

  showGridContent() {
    this.setState({
      isRouteToExperimentDetail: false,
      experimentDetail: undefined
    });
  }

  render() {
    setTimeout(() => {
      if (this.gridApi) {
        this.gridApi.sizeColumnsToFit();
      }
    }, 500);

    if (this.state.isRouteToExperimentDetail) {
      return <ExperimentDetail detail = { this.state.experimentDetail }
      navigateToParent = { this.showGridContent.bind(this) }/>;
    } else {
      return (
        <div className='landing-page'>
          <div className="ag-theme-material landing-page-grid">
            <AgGridReact
              columnDefs={this.state.columnDefs}
              rowData={this.state.experiments}
              context={this.state.context}
              frameworkComponents={this.state.frameworkComponents}
              onGridReady={this.onGridReady}
              rowHeight={this.state.rowHeight}
              headerHeight={this.state.headerHeight}
              onCellClicked={this.gridCellClick.bind(this)}
            >
            </AgGridReact>
          </div>
        </div>
      );
    }
  }
}
export default LandingPage;
LandingPage.propTypes = {
  data: PropTypes.any,
};
