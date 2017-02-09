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
import React, {PropTypes} from 'react';
import EntityCard from 'components/EntityCard';
import {parseMetadata} from 'services/metadata-parser';
import shortid from 'shortid';
require('./DataStreamCards.scss');

export default function DatasetStreamCards({dataEntities}) {
  let data = dataEntities.map( dataEntity => {
    let entity = {
      entityId: dataEntity.entityId,
      metadata: {
        SYSTEM: {}
      }
    };
    entity = parseMetadata(entity);
    entity.uniqueId = shortid.generate();
    return entity;
  });
  return (
    <div className="dataentity-cards">
      {
        data.map(dataEntity => (
          <EntityCard
            className="entity-card-container"
            entity={dataEntity}
            key={dataEntity.uniqueId}
          />
        ))
      }
    </div>
  );
}

DatasetStreamCards.propTypes = {
  dataEntities: PropTypes.arrayOf(PropTypes.object)
};
