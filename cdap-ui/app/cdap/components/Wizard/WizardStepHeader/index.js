/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {PropTypes} from 'react';
import classnames from 'classnames';
require('./WizardStepHeader.less');

export default function WizardStepHeader({id, className, label, onClick}, context) {
  return (
    <div
      className={classnames('cask-wizard-step-header', {
        'active': context.activeStep === id,
        [className]: context.activeStep !== id
      })}
      onClick={onClick}
    >
      {label}
    </div>
  );
}
WizardStepHeader.contextTypes = {
  activeStep: PropTypes.string
};
WizardStepHeader.propTypes = {
  id: PropTypes.string,
  className: PropTypes.string,
  label: PropTypes.string,
  onClick: PropTypes.func
};
