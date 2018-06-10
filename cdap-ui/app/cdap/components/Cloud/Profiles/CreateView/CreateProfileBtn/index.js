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

import PropTypes from 'prop-types';
import React from 'react';
import {connect} from 'react-redux';
import BtnWithLoading from 'components/BtnWithLoading';
import {isNilOrEmpty} from 'services/helpers';

function CreateProfileBtn({onClick, loading, disabled}) {
  return (
    <BtnWithLoading
      className="btn-primary"
      onClick={onClick}
      loading={loading}
      disabled={disabled}
      label="Create"
    />
  );
}

CreateProfileBtn.propTypes = {
  onClick: PropTypes.func,
  loading: PropTypes.bool,
  disabled: PropTypes.bool
};

function getDisabledState(properties, profileName, profileDescription) {
  let unfilledRequiredProperties = Object.keys(properties)
  .filter(prop => (
    properties[prop].required &&
    isNilOrEmpty(properties[prop].value)
  ));
  return !(
    profileName.length &&
    profileDescription.length &&
    !unfilledRequiredProperties.length
  );
}

const mapStateToProps = (state, ownProps) => {
  return {
    disabled: ownProps.loading || getDisabledState(state.properties, state.name, state.description),
    loading: ownProps.loading,
    onClick: ownProps.onClick
  };
};

const ConnecteCreateProfileBtn = connect(mapStateToProps)(CreateProfileBtn);

export default ConnecteCreateProfileBtn;
