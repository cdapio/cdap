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

import React, { Component, PropTypes } from 'react';
import {MySearchApi} from 'api/search';
import NamespaceStore from 'services/NamespaceStore';
import {parseMetadata} from 'services/metadata-parser';
import shortid from 'shortid';
import {objectQuery} from 'services/helpers';
import classnames from 'classnames';
import EntityCard from 'components/EntityCard';
import T from 'i18n-react';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';

require('./JustAddedSection.scss');

const TIME_THRESHOLD = 300000; // 5 minutes in millisecond

export default class JustAddedSection extends Component {
  constructor(props) {
    super(props);

    this.state = {
      entities: []
    };

    this.fetchEntities = this.fetchEntities.bind(this);
    this.eventEmitter = ee(ee);
    this.eventEmitter.on(globalEvents.APPUPLOAD, this.fetchEntities);
    this.eventEmitter.on(globalEvents.STREAMCREATE, this.fetchEntities);
    this.eventEmitter.on(globalEvents.PUBLISHPIPELINE, this.fetchEntities);
    this.eventEmitter.on(globalEvents.DELETEENTITY, this.fetchEntities);
    this.eventEmitter.on(globalEvents.ARTIFACTUPLOAD, this.fetchEntities);
    this.namespaceSub = NamespaceStore.subscribe(this.fetchEntities);
  }

  componentWillMount() {
    this.fetchEntities();
  }

  componentWillUnmount() {
    this.eventEmitter.off(globalEvents.APPUPLOAD, this.fetchEntities);
    this.eventEmitter.off(globalEvents.STREAMCREATE, this.fetchEntities);
    this.eventEmitter.off(globalEvents.PUBLISHPIPELINE, this.fetchEntities);
    this.eventEmitter.off(globalEvents.DELETEENTITY, this.fetchEntities);
    this.eventEmitter.off(globalEvents.ARTIFACTUPLOAD, this.fetchEntities);
    this.namespaceSub();
  }

  fetchEntities() {
    this.setState({loading: true});
    let namespace = NamespaceStore.getState().selectedNamespace;
    const params = {
      namespace,
      target: ['app', 'artifact', 'dataset', 'stream'],
      limit: this.props.limit,
      query: '*',
      sort: 'creation-time desc'
    };

    MySearchApi.search(params)
      .map((res) => {
        return res.results
          .map(parseMetadata)
          .filter((entity) => {
            let creationTime = objectQuery(entity, 'metadata', 'metadata', 'SYSTEM', 'properties', 'creation-time');

            creationTime = parseInt(creationTime, 10);
            let thresholdTime = Date.now() - TIME_THRESHOLD;
            return creationTime >= thresholdTime;
          })
          .map((entity) => {
            entity.uniqueId = shortid.generate();
            return entity;
          });
      })
      .subscribe((res) => {
        this.setState({
          entities: res,
          loading: false
        });
      }, (err) => {
        console.log('Error', err);
        this.setState({loading: false});
      });
  }

  render() {
    if (this.props.currentPage !== 1 || this.state.entities.length === 0 || this.state.loading) {
      return null;
    }

    let content = this.state.entities.map(entity => {
      return (
        <EntityCard
          className={
            classnames('entity-card-container',
              { active: entity.uniqueId === objectQuery(this.props, 'activeEntity', 'uniqueId') }
            )
          }
          key={entity.uniqueId}
          onClick={this.props.clickHandler.bind(this, entity)}
          entity={entity}
          onFastActionSuccess={this.props.onFastActionSuccess}
          onUpdate={this.props.onUpdate}
        />
      );
    });


    return (
      <div className="just-added-container">
        <div className="subtitle just-added">
          <span>
            {T.translate('features.EntityListView.JustAddedSection.subtitle')}
          </span>
        </div>

        <div className="just-added-entities-list">
          {content}
        </div>
      </div>
    );
  }
}

JustAddedSection.propTypes = {
  limit: PropTypes.number,
  clickHandler: PropTypes.func,
  onFastActionSuccess: PropTypes.func,
  onUpdate: PropTypes.func,
  currentPage: PropTypes.number
};
