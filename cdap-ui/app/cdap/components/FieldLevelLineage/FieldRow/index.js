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
import {getLineageSummary} from 'components/FieldLevelLineage/store/ActionCreator';
import classnames from 'classnames';
import {connect} from 'react-redux';

require('./FieldRow.scss');

function FieldRowView({fieldName, activeField}) {
  const isActive = fieldName === activeField;
  return (
    <div
      className={classnames('field-row', { 'active': isActive})}
      onClick={getLineageSummary.bind(null, fieldName)}
    >
      {fieldName}
    </div>
  );
}

FieldRowView.propTypes = {
  fieldName: PropTypes.string,
  activeField: PropTypes.string
};

const mapStateToProps = (state, ownProp) => {
  return {
    fieldName: ownProp.fieldName,
    activeField: state.lineage.activeField
  };
};

const FieldRow = connect(
  mapStateToProps
)(FieldRowView);

export default FieldRow;
