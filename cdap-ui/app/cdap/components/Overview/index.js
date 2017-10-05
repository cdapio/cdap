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
import AppOverview from 'components/Overview/AppOverview';
import DatasetOverview from 'components/Overview/DatasetOverview';
import StreamOverview from 'components/Overview/StreamOverview';
import {objectQuery} from 'services/helpers';
import isNil from 'lodash/isNil';
import classnames from 'classnames';
import SearchStore from 'components/EntityListView/SearchStore';
import SearchStoreActions from 'components/EntityListView/SearchStore/SearchStoreActions';
import {updateQueryString} from 'components/EntityListView/SearchStore/ActionCreator';
import {MyMetadataApi} from 'api/metadata';
import NamespaceStore from 'services/NamespaceStore';
import {convertEntityTypeToApi} from 'services/entity-type-api-converter';
import Mousetrap from 'mousetrap';
require('./Overview.scss');
import T from 'i18n-react';

export default class Overview extends Component {
  constructor(props) {
    super(props);
    this.state = {
      tag: null,
      entity: null,
      showOverview: false,
      loading: false
    };
    this.typeToComponentMap = {
      'application': AppOverview,
      'datasetinstance': DatasetOverview,
      'dataset': DatasetOverview,
      'stream': StreamOverview
    };
  }
  componentWillMount() {
    this.searchStoreSubscription = SearchStore.subscribe(() => {
      let searchState = SearchStore.getState().search;
      if (isNil(searchState.overviewEntity)) {
        this.setState({
          entity: null,
          showOverview: false,
          tag: null,
          loading: false,
          errorContent: null
        });
        return;
      }
      this.setState({
        showOverview: true,
        loading: true
      });
      let {id: entityId, type: entityType} = searchState.overviewEntity;
      let entityTypeLabel = entityType === 'datasetinstance' ? 'dataset' : entityType;
      entityType = convertEntityTypeToApi(entityType);
      let namespace = NamespaceStore.getState().selectedNamespace;

      MyMetadataApi
        .getMetadata({
          namespace,
          entityId,
          entityType
        })
        .subscribe(
          () => {
            this.setState({
              entity: searchState.overviewEntity,
              errorContent: null,
              showOverview: true,
              loading: false,
              tag: this.typeToComponentMap[objectQuery(searchState.overviewEntity, 'type')]
            }, this.scrollEntityToView.bind(this));
          },
          (err) => {
            let errorContent;
            if (err.statusCode === 404) {
              errorContent = this.get404ErrorMessage(entityId, entityTypeLabel);
            } else if (err.statusCode === 403) {
              errorContent = this.getAuthorizationMessage(entityId, entityTypeLabel);
            }
            this.setState({
              errorContent,
              loading: false,
              showOverview: true
            });
          }
        );
    });
  }
  componentDidMount() {
    this.bindKeyboardShortcuts();
    this.scrollEntityToView();
  }
  componentDidUpdate() {
    this.scrollEntityToView();
  }
  componentWillUnmount() {
    if (this.searchStoreSubscription) {
      this.searchStoreSubscription();
    }
    this.mousetrap.unbind('esc');
    this.mousetrap.reset();
  }
  scrollEntityToView() {
    if (isNil(this.state.entity) || !objectQuery(this.state, 'entity', 'uniqueId')) {
      return;
    }
    let el = document.getElementById(this.state.entity.uniqueId);
    if (isNil(el)) {
      return;
    }
    let paginationContainer = document.querySelector('.entity-list-view');
    el.scrollIntoView();
    if (paginationContainer.scrollTop < paginationContainer.scrollHeight - paginationContainer.offsetHeight) {
      paginationContainer.scrollTop -= 120;
    }
  }
  bindKeyboardShortcuts() {
    if (this.mousetrap) {
      this.mousetrap.reset();
      delete this.mousetrap;
    }
    this.mousetrap = new Mousetrap(document);
    this.mousetrap.bind('escape', (e) => {
      if (e.target.nodeName === 'BODY' && !document.querySelector('.modal')) {
        this.hideOverview();
      }
    });
  }
  get404ErrorMessage(entityId, entityTypeLabel) {
    return (
      <div className="overview-error-container">
        <h4>
          <strong>
            {T.translate('features.Overview.errorMessage404', {entityId, entityType: entityTypeLabel})}
          </strong>
        </h4>
        <hr />
        <div className="message-container">
          <span>{T.translate('features.EntityListView.emptyMessage.suggestion')}</span>
          <ul>
            <li>
              <span>
                {T.translate('features.Overview.errorMessageSubtitle')}
              </span>
            </li>
            <li>
              <span
                className="btn-link"
                onClick={this.hideOverview.bind(this)}
              >
                {T.translate('features.Overview.overviewCloseLabel')}
              </span>
              <span> {T.translate('features.Overview.overviewCloseLabel1')}</span>
            </li>
          </ul>
        </div>
      </div>
    );
  }
  getAuthorizationMessage(entityId, entityTypeLabel) {
    return (
      <div className="overview-error-container">
        <h4>
          <strong>
            {T.translate('features.Overview.errorMessageAuthorization', {entityId, entityType: entityTypeLabel})}
          </strong>
        </h4>
        <hr />
        <div className="message-container">
          <span>{T.translate('features.EntityListView.emptyMessage.suggestion')}</span>
          <ul>
            <li>
              <span>
                {T.translate('features.Overview.errorMessageSubtitle')}
              </span>
            </li>
            <li>
              <span
                className="btn-link"
                onClick={this.hideOverview.bind(this)}
              >
                {T.translate('features.Overview.overviewCloseLabel')}
              </span>
              <span> {T.translate('features.Overview.overviewCloseLabel1')}</span>
            </li>
          </ul>
        </div>
      </div>
    );
  }
  hideOverview() {
    this.setState({
      showOverview: false
    });
    SearchStore.dispatch({
      type: SearchStoreActions.RESETOVERVIEWENTITY
    });
    updateQueryString();
    this.mousetrap.unbind('esc');
  }
  closeAndRefresh(action) {
    this.hideOverview();
    if (action === 'delete') {
      if (this.props.onCloseAndRefresh) {
        this.props.onCloseAndRefresh();
      }
    }
  }
  render() {
    let Tag = this.state.tag || 'div';
    const renderContent = () => {
      if (this.state.errorContent) {
        return this.state.errorContent;
      }
      if (this.state.loading) {
        return (
          <div className="fa fa-spinner fa-spin fa-3x"></div>
        );
      }

      if (Tag === 'div') {
        return (<div></div>);
      }

      return React.createElement(
        Tag,
        {
          entity: this.state.entity,
          onClose: this.hideOverview.bind(this),
          onCloseAndRefresh: this.closeAndRefresh.bind(this)
        }
      );
    };
    return (
      <div className={classnames("overview-container", {"show-overview": this.state.showOverview })}>
        <div
          id="overview-wrapper"
          className="overview-wrapper"
        >
          {
            renderContent()
          }
        </div>
      </div>
    );
  }
}

Overview.propTypes = {
  onCloseAndRefresh: PropTypes.func
};
