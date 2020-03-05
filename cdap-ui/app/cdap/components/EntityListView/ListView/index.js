/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import EntityCard from 'components/EntityCard';
import classnames from 'classnames';
import JustAddedSection from 'components/EntityListView/JustAddedSection';
import NoEntitiesMessage from 'components/EntityListView/NoEntitiesMessage';
import SearchStore from 'components/EntityListView/SearchStore';
import SearchStoreActions from 'components/EntityListView/SearchStore/SearchStoreActions';
import ListViewHeader from 'components/EntityListView/ListViewHeader';
import { search, updateQueryString } from 'components/EntityListView/SearchStore/ActionCreator';
import {
  DEFAULT_SEARCH_SORT_OPTIONS,
  DEFAULT_SEARCH_QUERY,
  DEFAULT_SEARCH_FILTERS,
} from 'components/EntityListView/SearchStore/SearchConstants';
import isNil from 'lodash/isNil';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyAppApi } from 'api/app';

export default class HomeListView extends Component {
  constructor(props) {
    super(props);
    this.state = {
      loading: this.props.loading || false,
      list: this.props.list || [],
      applicationInfo: {},
    };
  }

  componentWillReceiveProps(nextProps) {
    this.fetchBatchApplicationsInfo(nextProps.list);
    this.setState({
      list: nextProps.list,
      loading: nextProps.loading,
    });
  }

  componentWillUnmount() {
    if (this.statusPoll$ && typeof this.statusPoll$.unsubscribe === 'function') {
      this.statusPoll$.unsubscribe();
    }
  }

  // TODO: CDAP-16192
  // Consolidate logic with JustAddedSection
  fetchBatchApplicationsInfo = (list) => {
    const apps = list
      .filter((entity) => entity.type === 'application')
      .map((app) => {
        return {
          appId: app.id,
        };
      });

    if (apps.length === 0) {
      return;
    }

    const params = {
      namespace: getCurrentNamespace(),
    };

    MyAppApi.batchAppDetail(params, apps).subscribe((res) => {
      const statusRequestBody = [];

      res.forEach((app) => {
        if (!app.detail || app.statusCode !== 200) {
          return;
        }

        const detail = app.detail;

        detail.programs.forEach((program) => {
          const programRequest = {
            appId: program.app,
            programType: program.type.toLowerCase(),
            programId: program.name,
          };

          statusRequestBody.push(programRequest);
        });
      });

      this.pollApplicationInfo(statusRequestBody);
    });
  };

  pollApplicationInfo = (requestBody) => {
    if (this.statusPoll$ && typeof this.statusPoll$.unsubscribe === 'function') {
      this.statusPoll$.unsubscribe();
    }

    const params = {
      namespace: getCurrentNamespace(),
    };

    this.statusPoll$ = MyAppApi.batchStatus(params, requestBody).subscribe((statusRes) => {
      const applicationInfo = {};

      statusRes.forEach((program) => {
        const appId = program.appId;
        if (!applicationInfo[appId]) {
          applicationInfo[appId] = {
            numPrograms: 0,
            running: 0,
            failed: 0,
          };
        }

        applicationInfo[appId].numPrograms++;

        if (program.status === 'RUNNING') {
          applicationInfo[appId].running++;
        } else if (program.status === 'FAILED') {
          applicationInfo[appId].failed++;
        }
      });

      this.setState({
        applicationInfo,
      });
    });
  };

  onClick(entity) {
    SearchStore.dispatch({
      type: SearchStoreActions.SETOVERVIEWENTITY,
      payload: {
        overviewEntity: {
          id: entity.id,
          type: entity.type,
          uniqueId: entity.uniqueId,
        },
      },
    });
    updateQueryString();
  }
  render() {
    let content;
    let searchState = SearchStore.getState().search;
    let query = searchState.query;
    let activeFilters = searchState.activeFilters;
    let filterOptions = searchState.filters;
    let overviewEntity = searchState.overviewEntity;
    let isEntityActive = (entity) => {
      if (isNil(overviewEntity)) {
        return false;
      }
      return (
        entity.id === overviewEntity.id &&
        entity.type === overviewEntity.type &&
        (overviewEntity.uniqueId === entity.uniqueId || isNil(overviewEntity.uniqueId)) // This will happen when the entity id and type comes from url and not through click
      );
    };
    if (this.state.loading) {
      content = (
        <h3 className="text-center">
          <span className="fa fa-spinner fa-spin fa-2x loading-spinner" />
        </h3>
      );
    }

    if (!this.state.loading && !this.state.list.length) {
      content = (
        <NoEntitiesMessage
          searchText={query}
          filtersAreApplied={() =>
            activeFilters.length > 0 && activeFilters.length < filterOptions.length
          }
          clearSearchAndFilters={() => {
            let searchState = SearchStore.getState().search;
            SearchStore.dispatch({
              type: SearchStoreActions.SETSORTFILTERSEARCHCURRENTPAGE,
              payload: {
                query: DEFAULT_SEARCH_QUERY,
                activeSort: DEFAULT_SEARCH_SORT_OPTIONS[4],
                activeFilters: DEFAULT_SEARCH_FILTERS,
                currentPage: 1,
                offset: searchState.offset,
                overviewEntity: null,
              },
            });
            search();
            updateQueryString();
          }}
        />
      );
    }
    if (!this.state.loading && this.state.list.length) {
      content = this.state.list.map((entity) => {
        let extraInfo;
        if (entity.type === 'application') {
          extraInfo = this.state.applicationInfo[entity.id];
        }

        return (
          <EntityCard
            className={classnames('entity-card-container', { active: isEntityActive(entity) })}
            id={entity.uniqueId}
            key={entity.uniqueId}
            onClick={this.onClick.bind(this, entity)}
            entity={entity}
            onFastActionSuccess={this.props.onFastActionSuccess}
            extraInfo={extraInfo}
          />
        );
      });
    }

    let currentPage = SearchStore.getState().search.currentPage;
    return (
      <div id={this.props.id} className={this.props.className}>
        {!this.props.showJustAddedSection ? null : (
          <JustAddedSection
            clickHandler={this.onClick.bind(this)}
            onFastActionSuccess={this.props.onFastActionSuccess}
            currentPage={currentPage}
            limit={this.props.pageSize}
          />
        )}
        <ListViewHeader />
        <div className="entities-all-list-container">{content}</div>
      </div>
    );
  }
}

HomeListView.propTypes = {
  list: PropTypes.array,
  loading: PropTypes.bool,
  onFastActionSuccess: PropTypes.func, // FIXME: This is not right. I don't think onFastActionSuccess is being used correct here. Not able to reason.
  className: PropTypes.string,
  pageSize: PropTypes.number,
  showJustAddedSection: PropTypes.bool,
  id: PropTypes.string,
};
