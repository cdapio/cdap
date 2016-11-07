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

import React, {Component, PropTypes} from 'react';
import EntityCard from 'components/EntityCard';
import {parseMetadata} from 'services/metadata-parser';
require('./DatasetsTab.less');

export default class DatasetsTab extends Component {
  constructor(props) {
    super(props);
  }
  render() {
    return (
      <div className="app-datasets-tab">
        {
          this.context
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
          this.context
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
        {
          !this.context.entity.datasets.length && !this.context.entity.streams.length ?
            <i className="fa fa-spin fa-spinner"></i>
          :
            null
        }
      </div>
    );
  }
}


DatasetsTab.contextTypes = {
  entity: PropTypes.object
};
