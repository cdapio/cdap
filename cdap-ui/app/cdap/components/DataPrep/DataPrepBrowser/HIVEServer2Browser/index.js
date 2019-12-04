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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import DataPrepBrowserStore from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import DataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import {Input} from 'reactstrap';
import ee from 'event-emitter';
import {objectQuery} from 'services/helpers';
import {setHiveServer2AsActiveBrowser, setError} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import DataPrepBrowserPageTitle from 'components/DataPrep/DataPrepBrowser/PageTitle';
import {Provider} from 'react-redux';
import DataprepBrowserTopPanel from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserTopPanel';
import {ConnectionType} from 'components/DataPrepConnections/ConnectionType';
import isNil from 'lodash/isNil';

require('./HIVEServer2Browser.scss');

const PREFIX = `features.DataPrep.DataPrepBrowser.HIVEServer2Browser`;

export default class HIVEServer2Browser extends Component {
  static propTypes = {
    toggle: PropTypes.func,
    onWorkspaceCreate: PropTypes.func
  };

  state = {
    info: DataPrepBrowserStore.getState().hiveserver2.info,
    connectionId: DataPrepBrowserStore.getState().hiveserver2.connectionId,
    connectionName: '',
    tables: [],
    loading: true,
    search: '',
    searchFocus: true
  };

  eventEmitter = ee(ee);

  componentDidMount() {
    this.eventEmitter.on('DATAPREP_CONNECTION_EDIT_HIVESERVER2', this.eventBasedFetchTable);
    this.storeSubscription = DataPrepBrowserStore.subscribe(() => {
      let {hiveserver2, activeBrowser} = DataPrepBrowserStore.getState();
      if (activeBrowser.name !== ConnectionType.HIVESERVER2) {
        return;
      }

      this.setState({
        info: hiveserver2.info,
        connectionId: hiveserver2.connectionId,
        loading: hiveserver2.loading,
        tables: hiveserver2.tables
      });
    });
  }

  componentWillUnmount() {
    this.eventEmitter.off('DATAPREP_CONNECTION_EDIT_HIVESERVER2', this.eventBasedFetchTable);

    if (typeof this.storeSubscription === 'function') {
      this.storeSubscription();
    }
  }

  eventBasedFetchTable = (connectionId) => {
    if (this.state.connectionId === connectionId) {
      setHiveServer2AsActiveBrowser({name: ConnectionType.HIVESERVER2, id: connectionId});
    }
  }

  handleSearch = (e) => {
    this.setState({
      search: e.target.value
    });
  }

  prepTable = (tableId) => {
    let namespace = NamespaceStore.getState().selectedNamespace;
    let params = {
      namespace,
      connectionId: this.state.connectionId,
      tableId,
      lines: 100
    };

    DataPrepApi.hiveserver2readTable(params)
      .subscribe(
        (res) => {
          let workspaceId = res.values[0].id;
          if (this.props.onWorkspaceCreate && typeof this.props.onWorkspaceCreate === 'function') {
            this.props.onWorkspaceCreate(workspaceId);
            return;
          }
          window.location.href = `${window.location.origin}/cdap/ns/${namespace}/dataprep/${workspaceId}`;
        },
        (err) => {
          let error = err;
          let errorMessage = T.translate(`${PREFIX}.defaultErrorMessage`, {tableId});
          if (isNil(objectQuery(err, 'response', 'message'))) {
            err = err || {};
            error = {
              ...err,
              response: {
                ...(err.response || {}),
                message: errorMessage
              }
            };
          }
          setError(error);
        }
      );
  }

  renderEmpty() {
    if (this.state.search.length !== 0) {
      return (
        <div className="empty-search-container">
          <div className="empty-search">
            <strong>
              {T.translate(`${PREFIX}.EmptyMessage.title`, {searchText: this.state.search})}
            </strong>
            <hr />
            <span> {T.translate(`${PREFIX}.EmptyMessage.suggestionTitle`)} </span>
            <ul>
              <li>
                <span
                  className="link-text"
                  onClick={() => {
                    this.setState({
                      search: '',
                      searchFocus: true
                    });
                  }}
                >
                  {T.translate(`${PREFIX}.EmptyMessage.clearLabel`)}
                </span>
                <span>{T.translate(`${PREFIX}.EmptyMessage.suggestion1`)}</span>
              </li>
            </ul>
          </div>
        </div>
      );
    }

    return (
      <div className="empty-search-container">
        <div className="empty-search text-xs-center">
          <strong>
            {T.translate(`${PREFIX}.EmptyMessage.emptyDatabase`, {connectionName: this.state.connectionName})}
          </strong>
        </div>
      </div>
    );
  }

  render() {
    const renderContents = (tables) => {
      if (!tables.length) {
        return this.renderEmpty();
      }
      return (
        <div className="hiveserver2-content-table">
          <div className="hiveserver2-content-header">
            <div className="row">
              <div className="col-xs-12">
                <span>{T.translate(`${PREFIX}.table.namecollabel`)}</span>
              </div>
            </div>
          </div>
          <div className="hiveserver2-content-body">
            {
              tables.map(table => {
                return (
                  <div
                    key={table.name}
                    className="row content-row"
                    onClick={this.prepTable.bind(this, table.name)}
                  >
                    <div className="col-xs-12">
                      <span>{table.name}</span>
                    </div>
                  </div>
                );
              })
            }
          </div>
        </div>
      );
    };

    if (this.state.loading) {
      return (
        <LoadingSVGCentered />
      );
    }
    let filteredTables = this.state.tables;
    if (this.state.search) {
      filteredTables = this.state.tables.filter(table => table.name.toLowerCase().indexOf(this.state.search.toLowerCase()) !== -1);
    }

    return (
      <div className="hiveserver2-browser">
        <Provider store={DataPrepBrowserStore}>
          <DataPrepBrowserPageTitle
            browserI18NName="HIVEServer2Browser"
            browserStateName="database"
          />
        </Provider>
        <DataprepBrowserTopPanel
          allowSidePanelToggle={true}
          toggle={this.props.toggle}
          browserTitle={T.translate(`${PREFIX}.title`)}
        />
        <div>
          <div className="hiveserver2-browser-header">
            <div className="hiveserver2-metadata">
              <h5>{objectQuery(this.state.info, 'info', 'name')}</h5>
              <span className="tables-count">
                {
                  T.translate(`${PREFIX}.tableCount`, {
                    context: this.state.tables.length
                  })
                }
              </span>
            </div>
            <div className="table-name-search">
              <Input
                placeholder={T.translate(`${PREFIX}.searchPlaceholder`)}
                value={this.state.search}
                onChange={this.handleSearch}
                autoFocus={this.state.searchFocus}
              />
            </div>
          </div>
        </div>

        <div className="hiveserver2-browser-content">
          { renderContents(filteredTables) }
        </div>
      </div>
    );
  }
}
