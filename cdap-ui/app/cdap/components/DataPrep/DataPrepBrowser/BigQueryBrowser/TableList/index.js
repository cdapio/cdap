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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import {listBiqQueryDatasets, listBigQueryTables} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import {getCurrentNamespace} from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import IconSVG from 'components/IconSVG';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import {Link} from 'react-router-dom';
import T from 'i18n-react';
import {objectQuery} from 'services/helpers';

const PREFIX = `features.DataPrep.DataPrepBrowser.BigQueryBrowser`;

class TableListView extends Component {
  static propTypes = {
    tableList: PropTypes.array,
    datasetId: PropTypes.string,
    connectionId: PropTypes.string,
    enableRouting: PropTypes.bool,
    match: PropTypes.object,
    loading: PropTypes.bool,
    onWorkspaceCreate: PropTypes.func
  };

  static defaultProps = {
    enableRouting: true
  };

  state = {
    loading: false
  };

  componentWillMount() {
    if (!this.props.enableRouting) { return; }

    let {
      datasetId,
      connectionId
    } = this.props.match.params;

    listBigQueryTables(connectionId, datasetId);
  }

  createWorkspace = (tableId) => {
    this.setState({
      loading: true
    });

    let namespace = getCurrentNamespace();

    let params = {
      namespace,
      connectionId: this.props.connectionId,
      datasetId: this.props.datasetId,
      tableId
    };

    MyDataPrepApi.readBigQueryTable(params)
      .subscribe((res) => {
        let workspaceId = objectQuery(res, 'values', 0, 'id');
        if (this.props.onWorkspaceCreate && typeof this.props.onWorkspaceCreate === 'function') {
          this.props.onWorkspaceCreate(workspaceId);
          return;
        }
        window.location.href = `${window.location.origin}/cdap/ns/${namespace}/dataprep/${workspaceId}`;
      });
  };

  render() {
    if (this.props.loading || this.state.loading) {
      return <LoadingSVGCentered />;
    }

    let Tag = this.props.enableRouting ? Link : 'span';
    let namespace = getCurrentNamespace();

    let path = `/ns/${namespace}/connections/bigquery/${this.props.connectionId}`;

    return (
      <div className="list-view-container">
        <div className="sub-panel">
          <div className="path">
            <Tag
              to={path}
              className="dataset-path"
              onClick={listBiqQueryDatasets.bind(null, this.props.connectionId)}
            >
              {T.translate(`${PREFIX}.datasets`)}
            </Tag>
            <span> / </span>
            <span>{this.props.datasetId}</span>
          </div>
        </div>

        <div className="list-table">
          <div className="table-header">
            <div className="row">
              <div className="col-xs-12">
                {T.translate(`${PREFIX}.name`)}
              </div>
            </div>
          </div>

          <div className="table-body">
            {
              this.props.tableList.map((table) => {
                return (
                  <div
                    onClick={this.createWorkspace.bind(this, table.id)}
                  >
                    <div className="row content-row">
                      <div className="col-xs-12">
                        <IconSVG name="icon-table" />
                        {table.id}
                      </div>
                    </div>
                  </div>
                );
              })
            }
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    tableList: state.bigquery.tableList,
    datasetId: state.bigquery.datasetId,
    connectionId: state.bigquery.connectionId,
    loading: state.bigquery.loading
  };
};

const TableList = connect(
  mapStateToProps
)(TableListView);

export default TableList;
