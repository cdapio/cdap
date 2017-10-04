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
import NamespaceStore from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import {Input} from 'reactstrap';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import isNil from 'lodash/isNil';
import {setKafkaAsActiveBrowser} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
import {objectQuery} from 'services/helpers';
import ee from 'event-emitter';

const PREFIX = `features.DataPrep.DataPrepBrowser.KafkaBrowser`;

require('./KafkaBrowser.scss');

export default class KafkaBrowser extends Component {
  constructor(props) {
    super(props);

    let store = DataPrepBrowserStore.getState();

    this.state = {
      connectionId: store.kafka.connectionId,
      info: store.kafka.info,
      loading: store.kafka.loading,
      search: '',
      searchFocus: true,
      error: null,
      topics: []
    };

    this.eventEmitter = ee(ee);
    this.handleSearch = this.handleSearch.bind(this);
    this.eventBasedFetchTopics = this.eventBasedFetchTopics.bind(this);

    this.eventEmitter.on('DATAPREP_CONNECTION_EDIT_KAFKA', this.eventBasedFetchTopics);
  }

  componentDidMount() {
    this.storeSubscription = DataPrepBrowserStore.subscribe(() => {
      let {kafka, activeBrowser} = DataPrepBrowserStore.getState();
      if (activeBrowser.name !== 'kafka') {
        return;
      }

      this.setState({
        info: kafka.info,
        connectionId: kafka.connectionId,
        topics: kafka.topics,
        error: kafka.error,
        loading: kafka.loading
      });
    });
  }

  componentWillUnmount() {
    this.eventEmitter.off('DATAPREP_CONNECTION_EDIT_KAFKA', this.eventBasedFetchTopics);
    if (this.storeSubscription) {
      this.storeSubscription();
    }
  }

  eventBasedFetchTopics(connectionId) {
    if (this.state.connectionId === connectionId) {
      setKafkaAsActiveBrowser({name: 'database', id: connectionId});
    }
  }

  handleSearch(e) {
    this.setState({
      search: e.target.value
    });
  }

  prepTopic(topic) {
    this.setState({
      loading: true
    });
    let namespace = NamespaceStore.getState().selectedNamespace;
    let params = {
      namespace,
      connectionId: this.state.connectionId,
      topic,
      lines: 100
    };

    MyDataPrepApi.readTopic(params)
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
          console.log('ERROR: ', err);
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
                <span> {T.translate(`${PREFIX}.EmptyMessage.suggestion1`)} </span>
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
            {T.translate(`${PREFIX}.EmptyMessage.emptyKafka`, {connectionName: this.state.connectionName})}
          </strong>
        </div>
      </div>
    );
  }

  renderError() {
    let error = this.state.error;
    let errorMessage = objectQuery(error, 'response', 'message') || objectQuery(error, 'response') || error;

    return (
      <div className="empty-search-container">
        <div className="empty-search">
          <strong>{errorMessage}</strong>
        </div>
      </div>
    );
  }

  renderContents(topics) {
    if (this.state.error) {
      return this.renderError();
    }
    if (!topics.length) {
      return this.renderEmpty();
    }
    return (
      <div className="kafka-content-table">
        <div className="kafka-content-header">
          <div className="row">
            <div className="col-xs-12">
              <span>{T.translate(`${PREFIX}.table.topics`)}</span>
            </div>
          </div>
        </div>
        <div className="kafka-content-body">
          {
            topics.map(topic => {
              return (
                <div
                  className="row content-row"
                  onClick={this.prepTopic.bind(this, topic)}
                  key={topic}
                >
                  <div className="col-xs-12">
                    <span>{topic}</span>
                  </div>
                </div>
              );
            })
          }
        </div>
      </div>
    );
  }

  render() {
    if (this.state.loading) {
      return (
        <LoadingSVGCentered />
      );
    }

    let filteredTopics = this.state.topics;
    if (this.state.search) {
      filteredTopics = this.state.topics.filter(topic => topic.toLowerCase().indexOf(this.state.search.toLowerCase()) !== -1);
    }

    return (
      <div className="kafka-browser">
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
              <div className="kafka-browser-header">
                <div className="kafka-metadata">
                  <h5>{this.state.info.name}</h5>
                  <span className="tables-count">
                    {
                      T.translate(`${PREFIX}.topicCount`, {
                        count: this.state.topics.length
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

        <div className="kafka-browser-content">
          { this.renderContents(filteredTopics) }
        </div>
      </div>
    );
  }
}

KafkaBrowser.propTypes = {
  toggle: PropTypes.func,
  onWorkspaceCreate: PropTypes.func
};
