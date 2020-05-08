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
import { MySearchApi } from 'api/search';
import NamespaceStore, { getCurrentNamespace } from 'services/NamespaceStore';
import { parseMetadata } from 'services/metadata-parser';
import uuidV4 from 'uuid/v4';
import { objectQuery } from 'services/helpers';
import classnames from 'classnames';
import EntityCard from 'components/EntityCard';
import T from 'i18n-react';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import SearchStore from 'components/EntityListView/SearchStore';
import { JUSTADDED_THRESHOLD_TIME } from 'components/EntityListView/SearchStore/SearchConstants';
import isNil from 'lodash/isNil';
import SearchStoreActions from 'components/EntityListView/SearchStore/SearchStoreActions';
import { SCOPES } from 'services/global-constants';
import { MyAppApi } from 'api/app';
require('./JustAddedSection.scss');

export default class JustAddedSection extends Component {
  constructor(props) {
    super(props);

    this.state = {
      entities: [],
      selectedEntity: {},
      applicationInfo: {},
    };

    this.fetchEntities = this.fetchEntities.bind(this);
    this.eventEmitter = ee(ee);
    this.eventEmitter.on(globalEvents.APPUPLOAD, this.fetchEntities);
    this.eventEmitter.on(globalEvents.PUBLISHPIPELINE, this.fetchEntities);
    this.eventEmitter.on(globalEvents.DELETEENTITY, this.fetchEntities);
    this.eventEmitter.on(globalEvents.ARTIFACTUPLOAD, this.fetchEntities);
    this.unmounted = false;
  }

  componentWillMount() {
    this.searchStoreSubscription = SearchStore.subscribe(() => {
      let overviewEntity = SearchStore.getState().search.overviewEntity;
      if (isNil(overviewEntity)) {
        this.setState({
          selectedEntity: {},
        });
        return;
      }
      let matchingEntity = this.state.entities
        // The unique id check to make sure not to highlight entities in both Just added section and the normal grid view.
        .find(
          (entity) =>
            entity.id === overviewEntity.id &&
            entity.type === overviewEntity.type &&
            entity.uniqueId === overviewEntity.uniqueId
        );
      if (matchingEntity) {
        this.setState({
          selectedEntity: matchingEntity,
        });
      } else {
        this.setState({
          selectedEntity: {},
        });
      }
    });
  }

  componentDidMount() {
    SearchStore.dispatch({
      type: SearchStoreActions.SETPAGESIZE,
      payload: {
        element: document.getElementsByClassName('entity-list-view'),
      },
    });
    this.fetchEntities();
  }
  componentWillUnmount() {
    this.eventEmitter.off(globalEvents.APPUPLOAD, this.fetchEntities);
    this.eventEmitter.off(globalEvents.PUBLISHPIPELINE, this.fetchEntities);
    this.eventEmitter.off(globalEvents.DELETEENTITY, this.fetchEntities);
    this.eventEmitter.off(globalEvents.ARTIFACTUPLOAD, this.fetchEntities);
    if (this.searchStoreSubscription) {
      this.searchStoreSubscription();
    }
    this.unmounted = true;

    if (this.statusPoll$ && typeof this.statusPoll$.unsubscribe === 'function') {
      this.statusPoll$.unsubscribe();
    }
  }

  fetchEntities() {
    this.setState({ loading: true });
    let namespace = NamespaceStore.getState().selectedNamespace;
    let numColumns = SearchStore.getState().search.numColumns;
    const params = {
      namespace,
      target: ['application', 'artifact', 'dataset'],
      limit: numColumns,
      query: '*',
      sort: 'creation-time desc',
      responseFormat: 'v6',
    };

    MySearchApi.search(params)
      .map((res) => {
        return res.results
          .map(parseMetadata)
          .filter((entity) => {
            let creationTime = objectQuery(entity, 'metadata', 'metadata', 'properties').find(
              (property) => property.name === 'creation-time' && property.scope === SCOPES.SYSTEM
            );
            creationTime = objectQuery(creationTime, 'value');
            creationTime = parseInt(creationTime, 10);
            let thresholdTime = Date.now() - JUSTADDED_THRESHOLD_TIME;
            return creationTime >= thresholdTime;
          })
          .map((entity) => {
            entity.uniqueId = uuidV4();
            return entity;
          });
      })
      .subscribe(
        (res) => {
          !this.unmounted &&
            this.setState({
              entities: res,
              loading: false,
            });

          this.fetchBatchApplicationsInfo(res);
        },
        (err) => {
          console.log('Error', err);
          !this.unmounted && this.setState({ loading: false });
        }
      );
  }

  // TODO: CDAP-16192
  // Consolidate logic with ListView
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
    this.setState({
      selectedEntity: entity,
    });
    this.props.clickHandler(entity);
  }
  render() {
    if (this.props.currentPage !== 1 || this.state.entities.length === 0 || this.state.loading) {
      return null;
    }

    let content = this.state.entities.map((entity) => {
      let extraInfo;
      if (entity.type === 'application') {
        extraInfo = this.state.applicationInfo[entity.id];
      }

      return (
        <EntityCard
          className={classnames('entity-card-container', {
            active: entity.uniqueId === objectQuery(this.state.selectedEntity, 'uniqueId'),
          })}
          key={entity.uniqueId}
          id={entity.uniqueId}
          onClick={this.onClick.bind(this, entity)}
          entity={entity}
          onFastActionSuccess={this.props.onFastActionSuccess}
          onUpdate={this.props.onUpdate}
          extraInfo={extraInfo}
        />
      );
    });

    return (
      <div className="just-added-container">
        <div className="subtitle just-added">
          <span>{T.translate('features.EntityListView.JustAddedSection.subtitle')}</span>
        </div>

        <div className="just-added-entities-list">{content}</div>
      </div>
    );
  }
}

JustAddedSection.propTypes = {
  limit: PropTypes.number,
  clickHandler: PropTypes.func,
  onFastActionSuccess: PropTypes.func,
  onUpdate: PropTypes.func,
  currentPage: PropTypes.number,
};
