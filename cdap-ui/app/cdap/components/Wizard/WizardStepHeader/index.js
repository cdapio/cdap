/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import classnames from 'classnames';
require('./WizardStepHeader.scss');

export default function WizardStepHeader({ id, className, label, onClick, disabled }, context) {
  return (
    <div
      className={classnames('cask-wizard-step-header', {
        active: context.activeStep === id,
        [className]: context.activeStep !== id,
        disabled: disabled,
      })}
    >
      <div onClick={onClick}>{label}</div>
    </div>
  );
}
WizardStepHeader.contextTypes = {
  activeStep: PropTypes.string,
};
WizardStepHeader.propTypes = {
  id: PropTypes.string,
  className: PropTypes.string,
  label: PropTypes.string,
  onClick: PropTypes.func,
  disabled: PropTypes.bool,
};
