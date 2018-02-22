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

import React from 'react';
import EntityCard from 'components/EntityCard';
import {parseMetadata} from 'services/metadata-parser';
import uuidV4 from 'uuid/v4';
require('./ProgramCards.scss');

export default function ProgramCards({programs}) {
  return (
    <div className="program-cards">
      {
        programs
          .map( program => {
            let entity = {
              entityId: {
                id: {
                  id: program.id,
                  application: {
                    applicationId: program.app
                  },
                  type: program.type
                },
                type: 'program',
              },
              metadata: {
                SYSTEM: {}
              }
            };
            entity = parseMetadata(entity);
            let uniqueId = uuidV4();
            entity.uniqueId = uniqueId;
            program.uniqueId = uniqueId;
            return (
              <EntityCard
                className="entity-card-container"
                entity={entity}
                key={program.uniqueId}
              />
            );
          })
      }
    </div>
  );
}
ProgramCards.propTypes = {
  programs: PropTypes.arrayOf(PropTypes.object)
};
