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
import classnames from 'classnames';
import SummaryRow from 'components/FieldLevelLineage/LineageSummary/SummaryRow';
import {getOperations} from 'components/FieldLevelLineage/store/ActionCreator';
import T from 'i18n-react';

const PREFIX = 'features.FieldLevelLineage.Summary';
const HEADERS_PREFIX = 'features.FieldLevelLineage.Headers';

require('./LineageSummary.scss');

const handleViewOperationsClick = (direction, summary) => {
  if (summary.length === 0) { return; }

  getOperations(direction);
};

export default function LineageSummary({activeField, datasetId, summary, direction}) {
  const datasetFieldTitle = `${datasetId}: ${activeField}`;

  if (activeField && summary.length === 0) {
    return (
      <div className="lineage-summary-empty-container">
        <span>
          {T.translate(`${PREFIX}.Empty.${direction}`, { fieldId: datasetFieldTitle })}
        </span>
      </div>
    );
  } else if (!activeField) {
    return (
      <div className="lineage-summary-empty-container">
        <span>
          {T.translate(`${PREFIX}.noFieldSelected`)}
        </span>
      </div>
    );
  }

  return (
    <div className="lineage-summary-container">
      <div className="field-lineage-info">
        <div className="title">
          <strong>
            {T.translate(`${PREFIX}.Title.${direction}`)}
          </strong>

          <span
            className="dataset-field truncate"
            title={datasetFieldTitle}
          >
            {datasetFieldTitle}
          </span>
        </div>

        <div className="lineage-count">
          {T.translate(`${PREFIX}.datasetCount`, { context: summary.length })}
        </div>
      </div>

      <div className="lineage-fields">
        <div className="lineage-column lineage-fields-header">
          <div className="index" />
          <div className="dataset-name">
            {T.translate(`${HEADERS_PREFIX}.datasetName`)}
          </div>
          <div className="field-name">
            {T.translate(`${HEADERS_PREFIX}.fieldName`)}
          </div>
        </div>

        <div className="lineage-fields-body">
          {
            summary.map((entity, i) => {
              return (
                <SummaryRow
                  key={i}
                  entity={entity}
                  index={i}
                />
              );
            })
          }
        </div>
      </div>

      <div className="view-operations">
        <span
          className={classnames({ disabled: summary.length === 0 })}
          onClick={handleViewOperationsClick.bind(null, direction, summary)}
        >
          {T.translate(`${PREFIX}.viewOperations`)}
        </span>
      </div>
    </div>
  );
}

LineageSummary.propTypes = {
  activeField: PropTypes.string,
  datasetId: PropTypes.string,
  summary: PropTypes.array,
  direction: PropTypes.oneOf(['incoming', 'outgoing'])
};
