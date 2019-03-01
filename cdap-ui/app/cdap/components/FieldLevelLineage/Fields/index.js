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
import { connect } from 'react-redux';
import FieldRow from 'components/FieldLevelLineage/Fields/FieldRow';
import FieldSearch from 'components/FieldLevelLineage/Fields/Search';
import SectionTitle from 'components/FieldLevelLineage/SectionTitle';
import Typography from '@material-ui/core/Typography';
import T from 'i18n-react';

require('./Fields.scss');

const PREFIX = 'features.FieldLevelLineage';

function FieldsView({ datasetId, fields, searchValue }) {
  let content = null;

  const emptyContent = (
    <div className="empty">{T.translate(`${PREFIX}.noFields`, { datasetId })}</div>
  );

  const emptySearch = <div className="empty">{T.translate(`${PREFIX}.noSearchFields`)}</div>;

  const listContent = (
    <div className="fields-list-body">
      {fields.map((field) => {
        return <FieldRow key={field.name} field={field} />;
      })}
    </div>
  );

  if (fields.length === 0 && searchValue.length > 0) {
    content = emptySearch;
  } else if (fields.length === 0) {
    content = emptyContent;
  } else {
    content = listContent;
  }

  return (
    <div className="fields-box">
      <SectionTitle entityId={datasetId} direction="self" />

      <div className="field-lineage-info">
        <Typography variant="caption" className="lineage-count">
          {T.translate(`${PREFIX}.fieldsCount`, { context: fields.length })}
        </Typography>

        <FieldSearch />
      </div>

      <div className="fields-list">
        <div className="fields-list-header field-row">
          <div className="operations" />
          <div className="field-name">{T.translate(`${PREFIX}.Headers.fieldName`)}</div>
          <div className="operations" />
        </div>

        {content}
      </div>
    </div>
  );
}

FieldsView.propTypes = {
  datasetId: PropTypes.string,
  fields: PropTypes.array,
  searchValue: PropTypes.string,
};

const mapStateToProps = (state) => {
  return {
    datasetId: state.lineage.datasetId,
    fields: state.lineage.fields,
    searchValue: state.lineage.search,
  };
};

const Fields = connect(mapStateToProps)(FieldsView);

export default Fields;
