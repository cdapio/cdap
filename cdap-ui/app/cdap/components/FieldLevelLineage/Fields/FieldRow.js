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

const onClickHandler = (field) => {
  if (!field.lineage) { return; }

  getLineageSummary(field.name);
};

function FieldRowView({field, activeField}) {
  const isActive = field.name === activeField;
  const isDisabled = !field.lineage;

  return (
    <div
      className={classnames('field-row truncate', {
        'active': isActive,
        'disabled': isDisabled
      })}
      onClick={onClickHandler.bind(null, field)}
      title={field.name}
    >
      {field.name}
    </div>
  );
}

FieldRowView.propTypes = {
  field: PropTypes.shape({
    name: PropTypes.string,
    lineage: PropTypes.bool
  }),
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
