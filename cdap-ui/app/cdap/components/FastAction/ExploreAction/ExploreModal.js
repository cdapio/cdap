/*
 * Copyright © 2016 Cask Data, Inc.
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
import {Modal, ModalHeader, ModalBody} from 'reactstrap';
import myExploreApi from 'api/explore';
import isObject from 'lodash/isObject';
import shortid from 'shortid';
import 'whatwg-fetch';
import {contructUrl, insertAt, removeAt, humanReadableDate} from 'services/helpers';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
require('./ExploreModal.scss');
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';

export default class ExploreModal extends Component {
  constructor(props) {
    super(props);
    this.type = this.props.entity.type === 'datasetinstance' ? 'dataset' : this.props.entity.type;
    this.state = {
      queryString: `SELECT * FROM ${this.props.entity.databaseName}.${this.props.entity.tableName} LIMIT 500`,
      queries: [],
      error: null,
      loading: false
    };
    // Show any queries that were executed when the modal is open, like `show tables`.
    // This is maintained in the current session and when the modal is opened again it doesn't need to be surfaced.
    this.sessionQueryHandles = [];
    this.submitQuery = this.submitQuery.bind(this);
    this.onQueryStringChange = this.onQueryStringChange.bind(this);
    this.subscriptions = [];
    this.updateState = this.updateState.bind(this);
  }
  componentWillUnmount() {
    this._mounted = false;
    this.subscriptions.map(subscriber => subscriber.unsubscribe());
  }
  componentWillReceiveProps(nextProps) {
    let {databaseName:existingDatabaseName, tableName:existingTableName} = this.props.entity;
    let {databaseName:newDatabaseName, tableName:newTablename} = nextProps.entity;
    if (existingDatabaseName !== newDatabaseName || existingTableName !== newTablename) {
      this.setState({
        queryString: `SELECT * FROM ${nextProps.entity.databaseName}.${nextProps.entity.tableName} LIMIT 500`,
      });
    }
  }
  updateState() {
    if (!this._mounted) {
      return;
    }
    this.setState.apply(this, arguments);
  }
  onQueryStringChange(e) {
    this.updateState({
      queryString: e.target.value
    });
  }
  setQueryString(query) {
    this.updateState({
      queryString: query.statement
    });
  }
  getValidQueries(queries) {
    let updatedQueries = queries
      .filter(q => {
        return (
          q.statement.indexOf( `${this.props.entity.databaseName}.${this.props.entity.tableName}`) !== -1 ||
          q.statement.indexOf( `${this.props.entity.id}`) !== -1 // For queries run before 4.1
        );
      });
    let updatedStateQueries = [...updatedQueries];
    let intersectingQueries = [];
    if (this.state.queries.length) {
      updatedStateQueries = this.state.queries.map(query => {
        let matchedQuery = updatedQueries.find(q => q.query_handle === query.query_handle);
        if (matchedQuery) {
          return Object.assign(query, {}, matchedQuery);
        }
        return query;
      });
      intersectingQueries = updatedQueries.filter(
        q => !this.state.queries.filter(
          qq => qq.query_handle === q.query_handle
        ).length
      );
    }

    return [
      ...intersectingQueries,
      ...updatedStateQueries
    ];
  }
  submitQuery() {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    this.setState({
      loading: true
    });
    let queriesSubscription$ = myExploreApi
      .submitQuery({namespace}, {query: this.state.queryString})
      .mergeMap((res) => {
        this.sessionQueryHandles.push(res.handle);
        return myExploreApi.fetchQueries({namespace});
      })
      .subscribe(
        (res) => {
          this.updateState({
            queries: this.getValidQueries(res),
            loading: false,
            error: null
          });
        },
        (error) => {
          this.updateState({
            error: isObject(error) ? error.response : error,
            loading: false
          });
        }
      );

    this.subscriptions.push(queriesSubscription$);
  }
  fetchAndUpdateQueries() {
    if (!this._mounted) {
      return;
    }
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let queriesSubscription$ = myExploreApi
      .fetchQueries({ namespace })
      .subscribe(
        (res = []) => {
          if (!res.length) {
            return;
          }
          let queries = this.getValidQueries(res);
          this.updateState({
            queries
          });
        },
        (error) => {
          this.updateState({
            error: isObject(error) ? error.response : error
          });
        }
      );
    this.subscriptions.push(queriesSubscription$);
  }
  componentWillMount() {
    this._mounted = true;
    this.fetchAndUpdateQueries();
  }
  showPreview(query) {
    let queryHandle = query.query_handle;
    let queries = this.state.queries;
    let matchIndex;
    queries.forEach((q, index) => {
      if (q.query_handle === queryHandle) {
        matchIndex = index;
      }
      return q;
    });
    if (queries[matchIndex + 1 ] && queries[matchIndex + 1 ].preview) {
      queries = removeAt(queries, matchIndex + 1);
      this.updateState({queries});
      return;
    }
    let previewSubscription$ = myExploreApi
      .getQuerySchema({queryHandle})
      .mergeMap(res => {
        queries = insertAt(queries, matchIndex, {
          schema: res.map(s => {
            if (s.name.indexOf('.') !== -1) {
              s.name = s.name.split('.')[1];
            }
            return s;
          })
        });
        this.updateState({
          queries
        });
        return myExploreApi.getQueryPreview({queryHandle});
      })
      .subscribe(res => {
        let matchIndex;
        queries.forEach((q, index) => {
          if (q.query_handle === queryHandle) {
            matchIndex = index;
          }
          return q;
        });
        queries[matchIndex + 1] = Object.assign(queries[matchIndex + 1], {preview: res});
        this.updateState({ queries });
      });
    this.subscriptions.push(previewSubscription$);
  }

  getDownloadUrl(query) {
    let path = `/data/explore/queries/${query.query_handle}/download`;
    path = encodeURIComponent(contructUrl({path}));
    let url = `/downloadLogs?backendUrl=${path}&type=download&method=POST&filename=${query.query_handle}.csv`;
    return url;
  }

  updateQueryState(currentQuery, event) {
    if (currentQuery.is_active === false) {
      event.preventDefault();
    } else {
      let queries = this.state.queries;
      queries.forEach((query, index) => {
        if (currentQuery == query) {
          currentQuery.is_active = false;
          queries[index] = currentQuery;
          this.updateState(this.state.queries);
        }
      });
    }
  }
  onModalToggle() {
    let runningQueries = this.state.queries.filter(query => query.status === 'RUNNING');
    this.props.onClose(runningQueries.length);
  }

  render() {
    const renderQueryRow = (query) => {
      let id = shortid.generate();
      return (
        <tr key={id}>
          <td> {humanReadableDate(query.timestamp, true)} </td>
          <td> {query.statement} </td>
          <td>
            {
              query.status === 'RUNNING' ?
                <span>
                  <span className="query-status-value">{query.status}</span>
                  <i className="fa fa-spinner fa-spin"></i>
                </span>
              :
                <span>{query.status}</span>
            }
          </td>
          <td>
            <div className="btn-group">
              {
                !query.is_active || query.status !== 'FINISHED' ?
                  <button
                    className="btn btn-secondary"
                    disabled="disabled"
                    >
                    <i
                      id={`${id}-download`}
                      className="fa fa-download"
                    ></i>
                    {
                      !query.is_active ?
                        <UncontrolledTooltip
                          target={`${id}-download`}
                          placement="left"
                          delay={300}
                        >
                          <div className="text-xs-left">
                            {T.translate('features.FastAction.downloadDisabledMessage')}
                          </div>
                        </UncontrolledTooltip>
                      :
                        null
                    }
                  </button>
                :
                  <a
                    href={this.getDownloadUrl(query)}
                    onClick={this.updateQueryState.bind(this, query)}
                    className="btn btn-secondary"
                  >
                    <i className="fa fa-download"></i>
                  </a>
              }
              <button
                className="btn btn-secondary"
                onClick={this.showPreview.bind(this, query)}
                disabled={!query.is_active || query.status !== 'FINISHED' ? 'disabled' : null}
              >
                <i
                  className="fa fa-eye"
                  id={`${id}-explore`}
                  delay={300}
                ></i>
              {
                !query.is_active?
                  <UncontrolledTooltip
                      target={`${id}-explore`}
                      placement="top"
                    >
                      <div className="text-xs-left">
                        {T.translate('features.FastAction.previewDisabledMessage')}
                      </div>
                    </UncontrolledTooltip>
                  :
                    null
              }
              </button>
              <button className="btn btn-secondary" onClick={this.setQueryString.bind(this, query)}>
                <i className="fa fa-clone"></i>
              </button>
            </div>
          </td>
        </tr>
      );
    };
    const renderPreviewRow = (query) => {
      const previewContent = (query) => {
        return (
          query.preview.length ?
            (
              <div>
                <table className="table table-bordered">
                  <thead>
                    <tr>
                      {
                        query.schema.map(s => (<th key={shortid.generate()}>{s.name}</th>))
                      }
                    </tr>
                  </thead>
                  <tbody>
                    {
                      query
                        .preview
                        .map((row) => {
                          return (
                            <tr key={shortid.generate()}>
                              {
                                !row.columns ?
                                  T.translate('features.FastAction.viewEvents.noResults')
                                :
                                  row.columns.map(column => {
                                    let content = column;

                                    if (content === null) {
                                      content = 'null';
                                    } else if (typeof content === 'boolean') {
                                      content = content === true ? 'true' : 'false';
                                    }

                                    return (
                                      <td key={shortid.generate()}>
                                        {content}
                                      </td>
                                    );
                                  })
                              }
                            </tr>
                          );
                        })
                    }
                  </tbody>
                </table>
              </div>
            )
          :
            <div className="text-xs-center">
              No Results
            </div>
        );
      };
      return (
        <tr key={shortid.generate()}>
          <td colSpan="4" className="preview-cell">
            {
              query.schema && !query.preview ?
                <div className="fa fa-spinner fa-spin text-xs-center"></div>
              :
                previewContent(query)
            }
          </td>
        </tr>
      );
    };
    return (
      <Modal
        className="explore-modal confirmation-modal"
        toggle={this.onModalToggle.bind(this)}
        isOpen={this.props.isOpen}
        backdrop='static'
      >
        <ModalHeader>
          { T.translate('features.FastAction.exploreLabel') }
          <div
           onClick={this.onModalToggle.bind(this)}
           className="float-xs-right"
          >
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          <div className="explore-modal-body">
            <textarea
              rows="5"
              className="form-control"
              value={this.state.queryString}
              onChange={this.onQueryStringChange}
            >
            </textarea>
            <div className="clearfix">
              {
                this.state.error ?
                  <span className="float-xs-left text-danger">{this.state.error}</span>
                :
                  null
              }
              <button
                className="btn btn-primary float-xs-right"
                onClick={this.submitQuery}
                disabled={this.state.loading ? 'disabled': null}
              >
                {
                  this.state.loading ?
                    <span className="fa fa-spinner fa-spin"></span>
                  :
                    null
                }
                <span>Execute</span>
              </button>
            </div>
            <div className="queries-table-wrapper">
              <table className="table table-bordered queries-table">
                <thead>
                  <tr>
                    <th className="query-timestamp">Start time</th>
                    <th>SQL Query</th>
                    <th className="query-status">Status</th>
                    <th className="query-actions">Actions</th>
                  </tr>
                </thead>
                <tbody>
                {
                  !this.state.queries.length ?
                    <tr>
                      <td
                        colSpan="4"
                        className="text-xs-center"
                      > No Results</td>
                    </tr>
                  :
                    this.state
                      .queries
                      .map((query) => {
                        if (query.preview || query.schema) {
                          return renderPreviewRow(query);
                        }
                        return renderQueryRow(query);
                      })
                }
                </tbody>
              </table>
            </div>
          </div>
        </ModalBody>
      </Modal>
    );
  }
}
ExploreModal.contextTypes = {
  params: PropTypes.shape({
    namespace: PropTypes.string
  })
};

ExploreModal.propTypes = {
  isOpen: PropTypes.bool,
  onClose: PropTypes.func,
  entity: PropTypes.shape({
    id: PropTypes.string.isRequired,
    version: PropTypes.string,
    scope: PropTypes.oneOf(['SYSTEM', 'USER']),
    type: PropTypes.oneOf(['application', 'artifact', 'datasetinstance', 'stream']).isRequired,
    databaseName: PropTypes.string,
    tableName: PropTypes.string
  })
};
