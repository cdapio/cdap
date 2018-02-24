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
import {listBigQueryTables, listBiqQueryDatasets} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import IconSVG from 'components/IconSVG';
import {Link} from 'react-router-dom';
import {getCurrentNamespace} from 'services/NamespaceStore';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import T from 'i18n-react';

const PREFIX = `features.DataPrep.DataPrepBrowser.BigQueryBrowser`;

class DatasetListView extends Component {
  static propTypes = {
    datasetList: PropTypes.array,
    connectionId: PropTypes.string,
    enableRouting: PropTypes.bool,
    loading: PropTypes.bool
  };

  static defaultProps = {
    enableRouting: true
  };

  componentWillMount() {
    if (this.props.connectionId) {
      listBiqQueryDatasets(this.props.connectionId);
    }
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.connectionId !== this.props.connectionId) {
      listBiqQueryDatasets(nextProps.connectionId);
    }
  }

  clickHandler = (datasetId) => {
    if (this.props.enableRouting) { return; }
    listBigQueryTables(this.props.connectionId, datasetId);
  };

  render() {
    if (this.props.loading) {
      return <LoadingSVGCentered />;
    }

    let namespace = getCurrentNamespace();

    return (
      <div className="list-view-container">
        <div className="sub-panel">
          {T.translate(`${PREFIX}.datasetCount`, {context: this.props.datasetList.length})}
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
              this.props.datasetList.map((dataset) => {
                let Tag = this.props.enableRouting ? Link : 'div';
                let path = `/ns/${namespace}/connections/bigquery/${this.props.connectionId}/datasets/${dataset.name}`;

                return (
                  <Tag
                    to={path}
                    onClick={this.clickHandler.bind(null, dataset.name)}
                  >
                    <div className="row content-row">
                      <div className="col-xs-12">
                        <IconSVG name="icon-database" />
                        {dataset.name}
                      </div>
                    </div>
                  </Tag>
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
    datasetList: state.bigquery.datasetList,
    connectionId: state.bigquery.connectionId,
    loading: state.bigquery.loading
  };
};

const DatasetList = connect(
  mapStateToProps
)(DatasetListView);

export default DatasetList;
