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
import {connect} from 'react-redux';
import FieldRow from 'components/FieldLevelLineage/FieldRow';
import LineageSummary from 'components/FieldLevelLineage/LineageSummary';
import FieldSearch from 'components/FieldLevelLineage/Fields/Search';
import T from 'i18n-react';

require('./Fields.scss');

const PREFIX = 'features.FieldLevelLineage';

function FieldsView({datasetId, fields}) {
  return (
    <div className="fields-list-container">
      <LineageSummary />
      <div className="fields-box">
        <div className="header">
          <div
            className="dataset-name truncate"
            title={datasetId}
          >
            {datasetId}
          </div>
          <div className="fields-count">
            {T.translate(`${PREFIX}.fieldsCount`, { context: fields.length })}
          </div>
        </div>

        <FieldSearch />

        <div className="fields-list">
          {
            fields.map((field) => {
              return (
                <FieldRow
                  key={field}
                  fieldName={field}
                />
              );
            })
          }
        </div>
      </div>
    </div>
  );
}

FieldsView.propTypes = {
  datasetId: PropTypes.string,
  fields: PropTypes.array
};

const mapStateToProps = (state) => {
  return {
    datasetId: state.lineage.datasetId,
    fields: state.lineage.fields
  };
};

const Fields = connect(
  mapStateToProps
)(FieldsView);

export default Fields;
