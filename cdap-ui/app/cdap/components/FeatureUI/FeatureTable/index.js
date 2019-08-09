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

import React from 'react';
import { AgGridReact } from 'ag-grid-react';
import 'ag-grid-community/dist/styles/ag-grid.css';
import 'ag-grid-community/dist/styles/ag-theme-balham.css';
import isEmpty from 'lodash/isEmpty';
import PropTypes from 'prop-types';
import { FAILED, DEPLOYED, SUCCEEDED, RUNNING, AFEGridColumns, EDIT, CLONE, DELETE } from '../config';
import StatusRenderer from '../GridRenderers/StatusRenderer';
import ActionRenderer from '../GridRenderers/ActionRenderer';
import FELinkRenderer from '../GridRenderers/FELinkRenderer';
import FSLinkRenderer from '../GridRenderers/FSLinkRenderer';

require('./FeatureTable.scss');

class FeatureTable extends React.Component {
  gridApi;
  gridColumnApi;
  constructor(props) {
    super(props);
    this.state = {
      columnDefs: AFEGridColumns,
      frameworkComponents: {
        'statusRenderer': StatusRenderer,
        'actionRenderer': ActionRenderer,
        'fsLinkRenderer': FSLinkRenderer,
        'feLinkRenderer': FELinkRenderer
      },
      context: { componentParent: this }
    };
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


  render() {
    setTimeout(() => {
      if (this.gridApi) {
        this.gridApi.sizeColumnsToFit();
      }
    }, 500);
    return (
      <div
        className="ag-theme-balham grid-container">
        <AgGridReact
          suppressMenuHide={true}
          columnDefs={this.state.columnDefs}
          context={this.state.context}
          frameworkComponents={this.state.frameworkComponents}
          rowData={this.props.data}
          enableFilter={true}
          onGridReady={this.onGridReady}
        >
        </AgGridReact>
      </div>
    );
  }
  getEpochDateString(epoch) {
    if (isNaN(epoch)) {
      return "—";
    } else {
      let date = new Date(epoch * 1000);
      return date.toDateString();
    }
  }

  onView(item) {
    if (this.props.onView) {
      this.props.onView(item);
    }
  }

  onFeatureSelection(item) {
    if (this.props.onFeatureSelection) {
      this.props.onFeatureSelection(item);
    }
  }

  onAction(item, type) {
    switch (type) {
      case EDIT:
        this.onEdit(item);
        break;
      case CLONE:
        this.onClone(item);
        break;
      case DELETE:
        this.onDelete(item);
        break;
    }
  }

  onEdit(item) {
    if (this.props.onEdit) {
      this.props.onEdit(item);
    }
  }

  onDelete(item) {
    if (this.props.onDelete) {
      this.props.onDelete(item);
    }
  }

  onClone(item) {
    if (this.props.onClone) {
      this.props.onClone(item);
    }
  }

  getStatusClass(item) {
    let className = "fa fa-circle right-padding";
    switch (item.status) {
      case SUCCEEDED:
        className += " status-success";
        break;
      case FAILED:
        className += " status-failed";
        break;
      case DEPLOYED:
        className += " status-deployed";
        break;
      case RUNNING:
        className += " status-running";
        break;
    }
    return className;
  }

}
export default FeatureTable;
FeatureTable.propTypes = {
  isDataLoading: PropTypes.any,
  data: PropTypes.array,
  onView: PropTypes.func,
  onFeatureSelection: PropTypes.func,
  onEdit: PropTypes.func,
  onDelete: PropTypes.func,
  onClone: PropTypes.func
};
