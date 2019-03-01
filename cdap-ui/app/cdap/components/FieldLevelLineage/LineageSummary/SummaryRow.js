/*
 * Copyright © 2018 Cask Data, Inc.
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

import React from 'react';
import PropTypes from 'prop-types';
import { Link } from 'react-router-dom';
import { getTimeQueryParams } from 'components/FieldLevelLineage/store/ActionCreator';

export default function SummaryRow({ entity, index }) {
  const linkPath = `/ns/${entity.dataset.namespace}/datasets/${
    entity.dataset.dataset
  }/fields?${getTimeQueryParams()}`;

  return (
    <div className="summary-row lineage-row">
      <div className="index">{index + 1}</div>

      <div className="dataset-name truncate">
        <Link to={linkPath} className="field-link" title={entity.dataset.dataset}>
          {entity.dataset.dataset}
        </Link>
      </div>

      <div className="field-name">
        {entity.fields.map((field, i) => {
          return (
            <div className="field-row truncate" key={i}>
              <Link to={`${linkPath}&field=${field}`} className="field-link" title={field}>
                {field}
              </Link>
            </div>
          );
        })}
      </div>
    </div>
  );
}

SummaryRow.propTypes = {
  entity: PropTypes.object,
  index: PropTypes.number,
};
