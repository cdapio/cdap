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
import SummaryRow from 'components/FieldLevelLineage/LineageSummary/SummaryRow';
import SectionTitle from 'components/FieldLevelLineage/SectionTitle';
import Typography from '@material-ui/core/Typography';
import T from 'i18n-react';

const PREFIX = 'features.FieldLevelLineage.Summary';
const HEADERS_PREFIX = 'features.FieldLevelLineage.Headers';

require('./LineageSummary.scss');

export default function LineageSummary({ activeField, datasetId, summary, direction }) {
  let content = null;
  const datasetFieldTitle = `${datasetId}: ${activeField}`;

  const empty = (
    <div className="lineage-summary-empty-container">
      <span>{T.translate(`${PREFIX}.Empty.${direction}`, { fieldId: datasetFieldTitle })}</span>
    </div>
  );

  const noActiveField = (
    <div className="lineage-summary-empty-container">
      <span>{T.translate(`${PREFIX}.noFieldSelected`)}</span>
    </div>
  );

  const lineageFields = (
    <div className="lineage-fields-body">
      {summary.map((entity, i) => {
        return <SummaryRow key={i} entity={entity} index={i} />;
      })}
    </div>
  );

  if (activeField && summary.length === 0) {
    content = empty;
  } else if (!activeField) {
    content = noActiveField;
  } else {
    content = lineageFields;
  }

  return (
    <div className="lineage-summary-container">
      <SectionTitle entityId={activeField} parentId={datasetId} direction={direction} />

      <div className="field-lineage-info">
        <Typography variant="caption" className="lineage-count">
          {T.translate(`${PREFIX}.datasetCount`, { context: summary.length })}
        </Typography>
      </div>

      <div className="lineage-fields">
        <div className="lineage-row lineage-fields-header">
          <div className="index" />
          <div className="dataset-name">{T.translate(`${HEADERS_PREFIX}.datasetName`)}</div>
          <div className="field-name">{T.translate(`${HEADERS_PREFIX}.fieldName`)}</div>
        </div>

        {content}
      </div>
    </div>
  );
}

LineageSummary.propTypes = {
  activeField: PropTypes.string,
  datasetId: PropTypes.string,
  summary: PropTypes.array,
  direction: PropTypes.oneOf(['incoming', 'outgoing']),
};
