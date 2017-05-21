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

import React, {Component, PropTypes} from 'react';
import DataPrepBrowserStore from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import isEqual from 'lodash/isEqual';
import DataPrepApi from 'api/dataprep';
import isNil from 'lodash/isNil';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import {Input} from 'reactstrap';
import IconSVG from 'components/IconSVG';

require('./DatabaseBrowser.scss');

const PREFIX = `features.DataPrep.DataPrepBrowser.DatabaseBrowser`;

export default class DatabaseBrowser extends Component {
  constructor(props) {
    super(props);
    let store = DataPrepBrowserStore.getState();
    this.state = {
      properties: store.database.properties,
      tables: [],
      loading: true,
      search: '',
      searchFocus: true,
      error: null
    };
    this.fetchTables = this.fetchTables.bind(this);
    this.handleSearch = this.handleSearch.bind(this);
    this.prepTable = this.prepTable.bind(this);
    this.fetchTables();
  }
  componentDidMount() {
    this.storeSubscription = DataPrepBrowserStore.subscribe(() => {
      let {database} = DataPrepBrowserStore.getState();
      if (database.loading) {
        this.setState({
          loading: true
        });
        return;
      }
      if (!isEqual(this.state.properties, database.properties)) {
        this.setState({
          properties: database.properties,
          loading: false
        }, this.fetchTables);
      }
    });
  }
  handleSearch(e) {
    this.setState({
      search: e.target.value
    });
  }
  prepTable(tableName) {
     let {userName, password, connectionString} = this.state.properties;
     let {selectedNamespace: namespace} = NamespaceStore.getState();
     let params = {
      namespace
    };
     let requestBody = {
       userName,
       password,
       connectionString,
       query: `SELECT * FROM ${tableName}`
     };
    DataPrepApi
      .readTable(params, requestBody)
      .subscribe(
        (res) => {
          let workspaceId = res.values[0].id;
          window.location.href = `${window.location.origin}/cdap/ns/${namespace}/dataprep/${workspaceId}`;
        },
        (err) => {
          console.log('ERROR: ', err);
        }
      );
  }
  fetchTables() {
    let {userName, password, connectionString} = this.state.properties;
    if (isNil(userName) || isNil(password) || isNil(connectionString)) {
      return;
    }
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let params = {
      namespace
    };
    DataPrepApi
      .listTables(params, {userName, password, connectionString})
      .subscribe(
        (res) => {
          this.setState({
            tables: res,
            loading: false
          });
        },
        (err) => {
          this.setState({
            error: typeof err === 'object' ? err.response : err,
            loading: false
          });
        }
      );
  }
  render() {
    // FIXME: This should be going away as user wouldn't even be reaching here if plugin is not installed
    const renderNoPluginMessage = () => {
      return (
         <div className="empty-search-container">
            <div className="empty-search">
              <strong>{T.translate(`${PREFIX}.NoPlugin.title`)}</strong>
              <hr />
              <span>{T.translate(`${PREFIX}.NoPlugin.suggestion1`)}</span>
            </div>
          </div>
      );
    };
    const renderContents = (tables) => {
      if (this.state.error) {
        return renderNoPluginMessage();
      }
      if (!tables.length) {
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
                  <span> {T.translate(`${PREFIX}.EmptyMessage.suggestion1`)} </span>
                </li>
              </ul>
            </div>
          </div>
        );
      }
      return (
        <div className="database-content-table">
          <div className="database-content-header">
            <div className="row">
              <div className="col-xs-8">
                <span>{T.translate(`${PREFIX}.table.namecollabel`)}</span>
              </div>
              <div className="col-xs-4">
                <span>{T.translate(`${PREFIX}.table.colcountlabel`)}</span>
              </div>
            </div>
          </div>
          <div className="database-content-body">
            {
              filteredTables.map(table => {
                return (
                  <div
                    className="row content-row"
                    onClick={this.prepTable.bind(this, table.tableName)}
                  >
                    <div className="col-xs-8">
                      <span>{table.tableName}</span>
                    </div>
                    <div className="col-xs-4">
                      <span>{table.columnCount}</span>
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
      filteredTables = this.state.tables.filter(table => table.tableName.indexOf(this.state.search) !== -1);
    }
    return (
      <div className="database-browser">
        <div className="top-panel">
          <div className="title">
            <h5>
              <span
                className="fa fa-fw"
                onClick={this.props.toggle}
              >
                <IconSVG name="icon-bars" />
              </span>

              <span>
                {T.translate(`${PREFIX}.title`)}
              </span>
            </h5>
          </div>
        </div>
        {
          isNil(this.state.error) ?
            <div>
              <div className="database-browser-header">
                <div className="database-metadata">
                  <h5>{this.state.properties.databasename}</h5>
                  <span className="tables-count">
                    {
                      T.translate(`${PREFIX}.tableCount`, {
                        count: this.state.tables.length
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
          :
            null
        }

        <div className="database-browser-content">
          { renderContents(filteredTables) }
        </div>
      </div>
    );
  }
}

DatabaseBrowser.propTypes = {
  toggle: PropTypes.func
};

