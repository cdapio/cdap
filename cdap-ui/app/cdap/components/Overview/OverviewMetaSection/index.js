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
import {objectQuery} from 'services/helpers';
import FastActions from 'components/EntityCard/FastActions';
import isNil from 'lodash/isNil';
import Description from 'components/Description';
import TimeAgo from 'react-timeago';
import Tags from 'components/Tags';
import moment from 'moment';
import uuidV4 from 'uuid/v4';
import T from 'i18n-react';

require('./OverviewMetaSection.scss');

export default class OverviewMetaSection extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entity: this.props.entity
    };
  }
  componentWillReceiveProps(nextProps) {
    let {entity} = nextProps;
    if (
      !isNil(entity) &&
      entity.id !== objectQuery(this.state, 'entity', 'id')
    ) {
      this.setState({
        entity
      });
    }
  }

  getFullCreationTime(creationTime) {
    // Sample formatted time string: Deployed on 02/16/2017, at 03:40 pm
    let formattedTime = moment(parseInt(creationTime)).format('[ on] MM/DD/YYYY, [at] hh:mm a');
    return formattedTime;
  }

  renderStreamInfo() {
    if (this.props.entity.type !== 'stream') { return null; }

    const TWENTY_YEARS = 20 * 365 * 24 * 60 * 60;

    let ttl = objectQuery(this.props, 'entity', 'properties', 'ttl');
    ttl = parseInt(ttl, 10);
    ttl = ttl < TWENTY_YEARS ? moment.duration(ttl).humanize() : 'Forever';

    return (
      <div className="entity-info">
        <strong>
          {T.translate('features.Overview.Metadata.ttl')}
        </strong>
        <span>{ttl}</span>
      </div>
    );
  }

  renderDatasetInfo() {
    if (this.props.entity.type !== 'datasetinstance') { return null; }

    let type = objectQuery(this.props, 'entity', 'properties', 'type');
    type = type.split('.');
    type = type[type.length - 1];

    return (
      <div className="entity-info">
        <strong>
          {T.translate('features.Overview.Metadata.type')}
        </strong>
        <span>{type}</span>
      </div>
    );
  }

  onFastActionSuccess(action) {
    if (this.props.onFastActionSuccess) {
      this.props.onFastActionSuccess(action);
    }
  }

  onFastActionUpdate(action) {
    if (this.props.onFastActionUpdate) {
      this.props.onFastActionUpdate(action);
    }
  }

  render() {
    let creationTime = objectQuery(this.props, 'entity', 'properties', 'creation-time');
    const renderCreationTime = (creationTime) => {
      return (
        this.props.showFullCreationTime ?
          <span>{this.getFullCreationTime(creationTime)}</span>
        :
          <TimeAgo date={parseInt(creationTime, 10)} />
      );
    };
    let description =  objectQuery(this.props, 'entity', 'properties', 'description');
    // have to generate new uniqueId here, because we don't want the fast actions here to
    // trigger the tooltips on the card view
    let entity = Object.assign({}, this.props.entity, {uniqueId: uuidV4()});
    return (
      <div className="overview-meta-section">
        <h2 title={this.props.entity.id}>
          {this.props.entity.id}
        </h2>
        <div className="fast-actions-container text-xs-center">
          <div>
            {
              this.props.entity.type === 'application' ?
                <span>
                  {
                    this.props.entity.properties.version === '-SNAPSHOT' ?
                      '1.0.0-SNAPSHOT'
                    :
                      this.props.entity.properties.version
                  }
                </span>
              :
                null
            }
            <small>
              {
                ['datasetinstance', 'stream'].indexOf(this.props.entity.type) !== -1 ?
                  T.translate('features.Overview.deployedLabel.data')
                :
                  T.translate('features.Overview.deployedLabel.app')
              }
              {
                !isNil(creationTime) ?
                  renderCreationTime(creationTime)
                :
                  null
              }
            </small>
          </div>
          <FastActions
            className="overview-fast-actions btn-group"
            entity={entity}
            onSuccess={this.onFastActionSuccess.bind(this)}
            onUpdate={this.onFastActionUpdate.bind(this)}
            actionToOpen={this.props.fastActionToOpen}
            argsToActions={{
              explore: {
                showQueriesCount: true
              }
            }}
          />
        </div>

        {this.props.showSeparator ? <hr /> : null}

        <Description description={description} />
        {this.renderDatasetInfo()}
        {this.renderStreamInfo()}
        <Tags entity={this.props.entity} />
      </div>
    );
  }
}

OverviewMetaSection.propTypes = {
  entity: PropTypes.object,
  onFastActionSuccess: PropTypes.func,
  onFastActionUpdate: PropTypes.func,
  fastActionToOpen: PropTypes.string,
  showFullCreationTime: PropTypes.bool,
  showSeparator: PropTypes.bool
};
