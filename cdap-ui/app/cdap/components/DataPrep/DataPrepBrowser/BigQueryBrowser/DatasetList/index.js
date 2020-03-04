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
import { connect } from 'react-redux';
import {
  listBigQueryTables,
  listBiqQueryDatasets,
} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import IconSVG from 'components/IconSVG';
import { Link } from 'react-router-dom';
import { getCurrentNamespace } from 'services/NamespaceStore';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import T from 'i18n-react';
import { objectQuery } from 'services/helpers';

const PREFIX = `features.DataPrep.DataPrepBrowser.BigQueryBrowser`;

class DatasetListView extends Component {
  static propTypes = {
    datasetList: PropTypes.array,
    connectionId: PropTypes.string,
    enableRouting: PropTypes.bool,
    loading: PropTypes.bool,
    match: PropTypes.object,
  };

  static defaultProps = {
    enableRouting: true,
  };

  state = {
    connectionId:
      this.props.connectionId || objectQuery(this.props, 'match', 'params', 'connectionId'),
  };

  componentDidMount() {
    if (!this.props.enableRouting) {
      return;
    }

    const { connectionId } = this.props.match.params;

    listBiqQueryDatasets(connectionId);
  }

  componentDidUpdate() {
    const connectionId =
      this.props.connectionId || objectQuery(this.props, 'match', 'params', 'connectionId');

    if (connectionId !== this.state.connectionId) {
      listBiqQueryDatasets(connectionId);
      this.setState({
        connectionId,
      });
    }
  }

  clickHandler = (datasetId) => {
    if (this.props.enableRouting) {
      return;
    }
    listBigQueryTables(this.props.connectionId, datasetId);
  };

  render() {
    if (this.props.loading) {
      return <LoadingSVGCentered />;
    }

    const datasetList = this.props.datasetList;

    if (!datasetList.length) {
      return (
        <div className="empty-search-container">
          <div className="empty-search text-center">
            <strong>
              {T.translate(`${PREFIX}.EmptyMessage.emptyDatasetList`, {
                connectionName: this.props.connectionId,
              })}
            </strong>
          </div>
        </div>
      );
    }

    let namespace = getCurrentNamespace();

    return (
      <div className="list-view-container">
        <div className="sub-panel">
          {T.translate(`${PREFIX}.datasetCount`, { context: datasetList.length })}
        </div>

        <div className="list-table">
          <div className="table-header">
            <div className="row">
              <div className="col-12">{T.translate(`${PREFIX}.name`)}</div>
            </div>
          </div>

          <div className="table-body">
            {datasetList.map((dataset) => {
              let Tag = this.props.enableRouting ? Link : 'div';
              let path = `/ns/${namespace}/connections/bigquery/${
                this.props.connectionId
              }/datasets/${dataset.name}`;

              return (
                <Tag
                  key={dataset.name}
                  to={path}
                  onClick={this.clickHandler.bind(null, dataset.name)}
                >
                  <div className="row content-row">
                    <div className="col-12">
                      <IconSVG name="icon-database" />
                      {dataset.name}
                    </div>
                  </div>
                </Tag>
              );
            })}
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    datasetList: state.bigquery.datasetList,
    connectionId: state.bigquery.connectionId,
    loading: state.bigquery.loading,
  };
};

const DatasetList = connect(mapStateToProps)(DatasetListView);

export default DatasetList;
