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
import T from 'i18n-react';
require('./DatasetTab.scss');
import {parseMetadata} from 'services/metadata-parser';
import EntityCard from 'components/EntityCard';
import {objectQuery} from 'services/helpers';

export default class DatasetTab extends Component {
  constructor(props) {
    super(props);
    this.state = {
      entity: this.props.entity
    };
  }
  componentWillReceiveProps(nextProps) {
    let entitiesMatch = objectQuery(nextProps, 'entity', 'name') === objectQuery(this.props, 'entity', 'name');
    if (!entitiesMatch) {
      this.setState({
        entity: nextProps.entity
      });
    }
  }
  render() {
    return (
      <div className="dataset-tab">
        <div className="message-section">
          <strong> {T.translate('features.Overview.DatasetTab.title', {appId: this.state.entity.name})} </strong>
        </div>
        {
          this.state
              .entity
              .datasets
              .map( dataset => {
                let entity = {
                  entityId: dataset.entityId,
                  metadata: {
                    SYSTEM: {}
                  }
                };
                entity = parseMetadata(entity);
                return (
                  <EntityCard
                    className="entity-card-container"
                    entity={entity}
                    key={dataset.uniqueId}
                  />
                );
              })
        }
        {
          this.state
              .entity
              .streams
              .map( stream => {
                let entity = {
                  entityId: stream.entityId,
                  metadata: {
                    SYSTEM: {}
                  }
                };
                entity = parseMetadata(entity);
                return (
                  <EntityCard
                    className="entity-card-container"
                    entity={entity}
                    key={stream.uniqueId}
                  />
                );
              })
        }
      </div>
    );
  }
}

DatasetTab.propTypes = {
  entity: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.string,
    datasets: PropTypes.arrayOf(PropTypes.shape({
      id: PropTypes.string,
      type: PropTypes.string
    })),
    streams: PropTypes.arrayOf(PropTypes.shape({
      id: PropTypes.string,
      type: PropTypes.string
    }))
  }))
};
